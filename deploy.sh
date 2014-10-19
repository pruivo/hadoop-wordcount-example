#!/bin/bash
set -e

HADOOP_MASTER=$1

mvn clean install 
sshpass -p "root" scp -o StrictHostKeyChecking=no target/hadoop-sample-1.0-SNAPSHOT-jar-with-dependencies.jar  root@$1:/home/hadoop/
exit 0
