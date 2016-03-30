#!/usr/bin/env python
"""
A FUSE wrapper for locally mounting Azure blob storage

Ahmet Alp Balkan <ahmetalpbalkan at gmail.com>
Pablo Saavedra <saavedra.pablo@gmail.com>
Aleksey Zhukov <drdaeman@drdaeman.pp.ru>

Assumes AzureFS API 2011-08-18 at least (x-ms-range)

https://msdn.microsoft.com/library/azure/ee691967.aspx
"""

import grp
import logging
import pwd
import time
import os
import sys
import errno
import stat
import re

from azure.common import AzureException, AzureHttpError,\
    AzureMissingResourceHttpError
from azure.storage.blob import BlobService
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from multiprocessing import Process, Manager


# http://stackoverflow.com/a/23364534/116546
RE_CONTAINER_NAME = r"^[a-z0-9](?:[a-z0-9]|(\\-(?!\\-))){1,61}[a-z0-9]$"
RFC822_TIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'


def rfc822_to_epoch(date):
    """
    Converts RFC-822/2822 ("Tue, 31 Jul 2012 07:17:34 GMT") string
    to an integer UNIX timestamp value.
    """
    return int(time.mktime(time.strptime(date, RFC822_TIME_FORMAT)))


def get_prop(obj, property_name, missing=None):
    """
    Tries to get object property, either stored as an attribute,
    or by treating object as a dict-like one, using getitem.

    This way we can unify querying over BlobProperties and dicts.
    """
    attribute_name = property_name.replace("-", "_")
    if hasattr(obj, attribute_name):
        return getattr(obj, attribute_name)
    try:
        return obj[property_name]
    except (TypeError, KeyError):
        return missing


def make_stat(mode, props=None):
    """
    Given an Properties object, or a dict similar to one fetched
    by get_blob_properties, return a dict with stat properties.
    """
    if props is None:
        props = dict()

    content_length = get_prop(props, 'content-length', 0)
    last_modified = get_prop(props, 'last-modified', None)
    if last_modified:
        last_modified = rfc822_to_epoch(last_modified)
    else:
        last_modified = int(time.time())

    return dict(st_mode=mode,
                st_uid=os.getuid(),
                st_size=content_length,
                st_nlink=1,
                st_mtime=last_modified)


def get_files_from_blob_service(blobs, cname, files):
    log.info("Getting files from AzureFS: %s" % cname)
    marker = None
    available_attempts = 5
    while True:
        try:
            batch = blobs.list_blobs(cname, marker=marker)
            log.info("Populating %s: received %s blobs, %s total",
                     cname, len(batch), len(files))

            for b in batch:
                blob_name = b.name
                node = make_stat(stat.S_IFREG | 0644, b.properties)
                if blob_name.find('/') == -1:  # file just under container
                    files[blob_name] = node
            if not batch.next_marker:
                break
            marker = batch.next_marker
        except Exception as e:
            log.exception("Remote connection error: %s", e)
            available_attempts -= 1
            if available_attempts is 0:
                log.warning("No more attempts to connect to Azure."
                            " Ending the thread.")
                break
            else:
                sleep_time = 5
                log.warning("Will retry again in %s seconds", sleep_time)
                time.sleep(sleep_time)


class AzureFS(LoggingMixIn, Operations):
    """
    Azure Blob Storage filesystem
    """

    blobs = None
    containers = dict()  # {cname: {stat:dict, files:None|{fname: stat}}
    fd = 0

    def __init__(self, account, key):
        self.blobs = BlobService(account, key)
        self._rebuild_container_list()

    def _rebuild_container_list(self):
        cmap = dict()
        cnames = set()

        for c in self.blobs.list_containers():
            cstat = make_stat(stat.S_IFDIR | 0755, c.properties)

            cname = c.name
            cmap['/' + cname] = dict(stat=cstat, files=None)
            cnames.add(cname)

        cmap['/'] = dict(files={}, stat=make_stat(stat.S_IFDIR | 0755))
        self.containers = cmap  # destroys fs tree cache resistant to misses

    @staticmethod
    def _parse_path(path):  # returns </dir, file(=None)>
        if path.count('/') > 1:  # file
            return str(path[:path.rfind('/')]), str(path[path.rfind('/') + 1:])
        else:  # dir
            pos = path.rfind('/', 1)
            if pos == -1:
                return path, None
            else:
                return str(path[:pos]), None

    @staticmethod
    def _parse_container(path):
        base_container = path[1:]  # /abc/def/g --> abc
        if base_container.find('/') > -1:
            base_container = base_container[:base_container.find('/')]
        return str(base_container)

    def _get_dir(self, path, contents_required=False, force=False):
        log.debug("get_dir: contents_required=%s, force=%s,"
                  " has_container=%s, path=%s",
                  "t" if contents_required else "f",
                  "t" if force else "f",
                  "t" if path in self.containers else "f",
                  path)
        cname = self._parse_container(path)

        if 'process' in self.containers['/' + cname] and \
                self.containers['/' + cname]['process'] is not None:
            p = self.containers['/' + cname]['process']
            if not p.is_alive():
                p.join()
                self.containers['/' + cname]['process'] = None

        if not self.containers:
            log.info("get_dir: rebuilding container list")
            self._rebuild_container_list()

        if path in self.containers:
            container = self.containers[path]
            if not contents_required:
                return container
            if not force and container['files'] is not None:
                return container

        if '/' + cname not in self.containers:
            log.info("get_dir: no such container: /%s", cname)
            raise FuseOSError(errno.ENOENT)
        else:
            container = self.containers['/' + cname]
            try:
                log.info(">>>> %s - %s ",
                         cname,
                         container['process'])
            except KeyError:
                log.info(">>>> no process: %s " % cname)
            if container['files'] is None or force is True:
                # fetch contents of container
                log.info("Contents not found in the cache index: %s", cname)

                process = container.get('process', None)
                if process is not None and process.is_alive():
                    # We do nothing. Some thread is still working,
                    # getting list of blobs from the container.
                    log.info("Fetching blob list for '%s' is already"
                             " handled by %s", cname, process)
                else:
                    # No thread running for this container, launch a new one
                    m = Manager()
                    files = m.dict()
                    process = Process(target=get_files_from_blob_service,
                                      args=(self.blobs, cname, files),
                                      name="list-blobs/%s" % cname)
                    process.daemon = True
                    process.start()
                    container['process'] = process
                    log.info("Started blob list retrieval for '%s': %s",
                             cname, process)
                    container['files'] = files
            return container

    def _get_file(self, path):
        d, f = self._parse_path(path)
        log.debug("get_file: requested path=%s (d=%s, f=%s)", path, d, f)
        directory = self._get_dir(d, True)
        files = None
        if directory is not None:
            files = directory['files']
            if f in files:
                return files[f]

        if not hasattr(self, "_get_file_noent"):
            self._get_file_noent = {}

        last_check = self._get_file_noent.get(path, 0)
        if time.time() - last_check <= 30:
            # Negative TTL is 30 seconds (hardcoded for now)
            log.info("get_file: cache says to reply negative for %s", path)
            return None

        # Check if file now exists and our caches are just stale.
        try:
            c = self._parse_container(d)
            p = path[path.find('/', 1) + 1:]
            props = self.blobs.get_blob_properties(c, p)
            log.info("get_file: found locally unknown remote file %s: %s",
                     path, repr(props))

            node = make_stat(stat.S_IFREG | 0644, props)

            if node['st_size'] > 0:
                log.info("get_file: properties for %s: %s", path, repr(node))
                # Remember this, so we won't have to re-query it.
                files[f] = node
                if path in self._get_file_noent:
                    del self._get_file_noent[path]
                return node
            else:
                # TODO: FIXME: HACK: We currently ignore empty files.
                # Sometimes the file is not yet here and is still uploading.
                # Such files have "content-length: 0". Ignore those for now.
                log.warning("get_file: the file %s is not yet here (size=%s)",
                            path, node['st_size'])
                self._get_file_noent[path] = time.time()
                return None
        except AzureMissingResourceHttpError:
            log.info("get_file: remote confirms non-existence of %s", path)
            self._get_file_noent[path] = time.time()
            return None
        except AzureException as e:
            log.error("get_file: exception while querying remote for %s: %s",
                      path, repr(e))
            self._get_file_noent[path] = time.time()

        return None

    def getattr(self, path, fh=None):
        log.debug("getattr: path=%s", path)
        d, f = self._parse_path(path)

        if f is None:
            return self._get_dir(d)['stat']
        else:
            file_obj = self._get_file(path)
            if file_obj:
                return file_obj

        log.warning("getattr: no such file: %s", path)
        raise FuseOSError(errno.ENOENT)

    def mkdir(self, path, mode):
        if path.count('/') <= 1:  # create on root
            name = path[1:]
            if not 3 <= len(name) <= 63:
                log.error("Container names can be 3 through 63 chars long")
                raise FuseOSError(errno.ENAMETOOLONG)

            if not re.match(RE_CONTAINER_NAME, name):
                log.error("Invalid container name: '%s'", name)
                raise FuseOSError(errno.EACCES)

            resp = self.blobs.create_container(name)
            if resp:
                self._rebuild_container_list()
                log.info("CONTAINER %s CREATED", name)
            else:
                log.error("Invalid container name or container already exists")
                raise FuseOSError(errno.EACCES)
        else:
            # TODO: Support 2nd+ level directory creation
            raise FuseOSError(errno.ENOSYS)

    def rmdir(self, path):
        if path.count('/') == 1:
            c_name = path[1:]
            resp = self.blobs.delete_container(c_name)

            if resp:
                if path in self.containers:
                    del self.containers[path]
            else:
                raise FuseOSError(errno.EACCES)
        else:
            # TODO: Support 2nd+ level directories
            raise FuseOSError(errno.ENOSYS)

    def create(self, path, mode, fi=None):
        node = make_stat(stat.S_IFREG | mode)
        d, f = self._parse_path(path)

        if not f:
            log.error("Cannot create files on root level: /")
            raise FuseOSError(errno.ENOSYS)

        if f == ".__refresh_cache__":
            log.info("Refresh cache forced (%s)" % f)
            self._get_dir(path, True, True)
            return self.open(path, data='')

        directory = self._get_dir(d, True)
        if not directory:
            raise FuseOSError(errno.EIO)
        directory['files'][f] = node

        return self.open(path, data='')  # reusing handler provider

    def open(self, path, flags=0, data=None):
        log.info("open: path=%s; flags=%s", path, flags)
        if data is None:
            # Download contents
            c_name = self._parse_container(path)
            f_name = path[path.find('/', 1) + 1:]

            try:
                self.blobs.get_blob_metadata(c_name, f_name)
            except AzureMissingResourceHttpError:
                directory = self._get_dir('/' + c_name, True)
                if f_name in directory['files']:
                    del directory['files'][f_name]
                log.info("open: remote says there is no such file: c=%s f=%s",
                         c_name, f_name)
                raise FuseOSError(errno.ENOENT)
            except AzureHttpError as e:
                log.error("Read blob failed with HTTP %d", e.status_code)
                raise FuseOSError(errno.EAGAIN)
            except AzureException as e:
                log.exception("Read blob failed with exception: %s", repr(e))
                raise FuseOSError(errno.EAGAIN)
        self.fd += 1
        return self.fd

    def truncate(self, path, length, fh=None):
        return 0  # assume done, no need

    def write(self, path, data, offset, fh=None):
        # TODO: Re-implement writing
        raise FuseOSError(errno.EPERM)

    def unlink(self, path):
        c_name = self._parse_container(path)
        d, f = self._parse_path(path)

        try:
            self.blobs.delete_blob(c_name, f)

            _dir = self._get_dir(path, True)
            if _dir and f in _dir['files']:
                del _dir['files'][f]
            return 0
        except AzureMissingResourceHttpError:
            raise FuseOSError(errno.ENOENT)
        except:
            raise FuseOSError(errno.EAGAIN)

    def readdir(self, path, fh):
        if path == '/':
            return ['.', '..'] + [x[1:] for x in self.containers.keys()
                                  if x != '/']

        directory = self._get_dir(path, True)
        if not directory:
            log.info("readdir: no such file: %s", path)
            raise FuseOSError(errno.ENOENT)
        return ['.', '..'] + directory['files'].keys()

    def read(self, path, size, offset, fh):
        f_name = path[path.find('/', 1) + 1:]
        c_name = path[1:path.find('/', 1)]

        try:
            byte_range = "bytes=%s-%s" % (offset, offset + size - 1)
            log.debug("read range: %s", byte_range)
            data = self.blobs.get_blob(c_name, f_name, snapshot=None,
                                       x_ms_range=byte_range)
            return data
        except AzureHttpError as e:
            if e.status_code == 404:
                raise FuseOSError(errno.ENOENT)
            elif e.status_code == 403:
                raise FuseOSError(errno.EPERM)
            else:
                log.error("Read blob failed with HTTP %d", e.status_code)
                raise FuseOSError(errno.EAGAIN)

    def statfs(self, path):
        return dict(f_bsize=4096, f_blocks=1, f_bavail=sys.maxint)

    def rename(self, old, new):
        # TODO: Implement renaming
        raise FuseOSError(errno.ENOSYS)

    def symlink(self, target, source):
        raise FuseOSError(errno.ENOSYS)

    def getxattr(self, path, name, position=0):
        return ''

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass


if __name__ == '__main__':
    import argparse

    # Command-line argument parsing
    description = ''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('account', metavar='AzureAccount',
                        help='Azure account')
    parser.add_argument('secretkey', metavar='AzureSecretKey',
                        help='Azure secret key')
    parser.add_argument('mountpoint', metavar='LocalPath',
                        help='the local mount point')

    parser.add_argument('--mkdir', action='store_true',
                        help='create mountpoint if not found (and create'
                             ' intermediate directories as required)')
    parser.add_argument('--nonempty', action='store_true',
                        help='allows mounts over a non-empty file'
                             ' or directory')
    parser.add_argument('--uid', metavar='N',
                        help='default UID')
    parser.add_argument('--gid', metavar='N',
                        help='default GID')
    parser.add_argument('--umask', metavar='MASK',
                        help='default umask')
    parser.add_argument('--read-only', action='store_true',
                        help='mount read only')

    parser.add_argument('--no-allow-other', action='store_true',
                        help='Do not allow other users to access'
                             ' mounted directory')

    parser.add_argument('-f', '--foreground', action='store_true',
                        help='run in foreground')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show debug info')

    options = parser.parse_args()

    # Logging setup
    fmt = logging.Formatter(fmt="[%(asctime)s] P%(process)d,T%(thread)d"
                                " %(levelname)s: %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger("azurefs")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(ch)
    log.setLevel(logging.DEBUG if options.debug else logging.INFO)
    log.debug("options = %s", options)

    if options.mkdir and not os.path.exists(options.mountpoint):
        os.makedirs(options.mountpoint)

    mount_options = {
        'mountpoint': options.mountpoint,
        'fsname': 'azurefs',
        'foreground': options.foreground,
        'allow_other': True,
        'auto_cache': True,
        'atime': False,
        'max_read': 131072,
        'max_write': 131072,
        'max_readahead': 131072,
    }

    if options.no_allow_other:
        mount_options["allow_other"] = False
    if options.uid:
        try:
            mount_options['uid'] = pwd.getpwnam(options.uid).pw_uid
        except KeyError:
            mount_options['uid'] = options.uid
    if options.gid:
        try:
            mount_options['gid'] = grp.getgrnam(options.gid).gr_gid
        except KeyError:
            mount_options['gid'] = options.gid
    if options.umask:
        mount_options['umask'] = options.umask
    if options.read_only:
        mount_options['ro'] = True

    if options.nonempty:
        mount_options['nonempty'] = True

    mount_options['nothreads'] = False

    log.debug("mount options: %s", mount_options)
    fuse = FUSE(AzureFS(options.account, options.secretkey), **mount_options)
