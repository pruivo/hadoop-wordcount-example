#!/bin/bash

WORK_DIR=`cd $(dirname $0); pwd`
HADOOP_INTEGRATION_DIR=/home/pedro/IdeaProjects/infinispan-hadoop-integration

cd ${HADOOP_INTEGRATION_DIR}
mvn clean install
cd ${WORK_DIR}
mvn clean install

exit 0
