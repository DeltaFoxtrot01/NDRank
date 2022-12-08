#!/bin/bash

#make sure it is in the original location
cd /home/pweather

#download and decompress kafka 
wget https://downloads.apache.org/kafka/3.2.3/kafka-3.2.3-src.tgz
tar -xvf kafka-3.2.3-src.tgz

#copy files to configure kafka
cp /home/pweather/pweather-brute-force-ndrank/packer/kafka/zookeeper-script.sh /home/pweather/zookeeper-script.sh
cp /home/pweather/pweather-brute-force-ndrank/packer/kafka/zookeeper.service /home/pweather/zookeeper.service
cp /home/pweather/pweather-brute-force-ndrank/packer/kafka/kafka-script.sh /home/pweather/kafka-script.sh
cp /home/pweather/pweather-brute-force-ndrank/packer/kafka/kafka.service /home/pweather/kafka.service
cp /home/pweather/pweather-brute-force-ndrank/packer/startup_scripts/kafka_merger_script.sh /home/pweather/execute_server.sh
cp /home/pweather/pweather-brute-force-ndrank/packer/startup_scripts/kafka_merger.service /home/pweather/kafka_merger.service


echo "message.max.bytes=209715200" >> /home/pweather/kafka-3.1.0-src/config/server.properties

#install java
wget https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz
tar xvf openjdk-17.0.2_linux-x64_bin.tar.gz
sudo mv jdk-17.0.2/ /opt/jdk-17/
export JAVA_HOME=/opt/jdk-17
export PATH=$PATH:$JAVA_HOME/bin

#configure zookeeper
cd /home/pweather/kafka-3.2.3-src
./gradlew jar -PscalaVersion=2.13.6
chmod +x /home/pweather/zookeeper-script.sh
sudo cp /home/pweather/zookeeper.service /etc/systemd/system/zookeeper.service
sudo systemctl enable zookeeper.service

#configure kafka
chmod +x /home/pweather/kafka-script.sh
sudo cp /home/pweather/kafka.service /etc/systemd/system/kafka.service
sudo systemctl enable kafka.service

#configure kafka merger server
chmod +x /home/pweather/execute_server.sh
sudo cp /home/pweather/kafka_merger.service /etc/systemd/system/kafka_merger.service
sudo systemctl enable kafka_merger.service