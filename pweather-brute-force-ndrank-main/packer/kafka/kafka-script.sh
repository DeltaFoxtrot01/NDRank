#!/bin/bash
export JAVA_HOME=/opt/jdk-17
export PATH=$PATH:$JAVA_HOME/bin

/home/pweather/kafka-3.1.0-src/bin/kafka-server-start.sh /home/pweather/kafka-3.1.0-src/config/server.properties