#to configure the master's parameter disk as to mount automatically
mkdir /home/pweather/parameters
#/dev/disk/by-id/google-params  /home/pweather/parameters   ext4    errors=continue 0   2
sudo chmod 666 /etc/fstab
sudo echo "/dev/disk/by-id/google-params    /home/pweather/parameters   ext4    errors=continue 0   2" >> /etc/fstab