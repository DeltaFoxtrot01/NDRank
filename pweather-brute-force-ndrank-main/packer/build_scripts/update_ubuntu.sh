#!/bin/bash
#interval to avoid race condition
sleep 30


#install regular linux packages
sudo apt update -y
sudo apt install dialog apt-utils -y
sudo apt upgrade -y
sudo apt install git -y
sudo apt install wget -y
sudo apt install kafkacat -y
