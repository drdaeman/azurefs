# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.provision "shell", inline: <<-SHELL
     sudo apt-get update
     sudo apt-get install -y python-pip fuse libfuse2 libfuse-dev
     sudo pip install fusepy azure
     sudo sed -ie 's/^#\s*\(user_allow_other\)/\1/' /etc/fuse.conf
     sudo gpasswd -a vagrant fuse
     [ -e /home/vagrant/mnt ] || mkdir /home/vagrant/mnt
     chmod 0750 /home/vagrant/mnt
  SHELL
end
