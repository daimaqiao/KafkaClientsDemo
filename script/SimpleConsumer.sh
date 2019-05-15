#!/usr/bin/env bash

JAR=`ls -1t *.jar |grep -e "^KafkaClientsDemo-.*\.jar$" |head -1`
if [ -z "$JAR" ]; then
    exit 1
fi

java -cp $JAR dmq.test.kafka.SimpleProducer
