#!/bin/bash
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

rm -rf ./data
rm netflix-prize-data.zip

kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --delete \
 --topic kafka-input

kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --create \
 --replication-factor 2 --partitions 3 --topic kafka-input

hadoop fs -copyToLocal netflix-prize-data.zip
unzip netflix-prize-data.zip

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer data/netflix-prize-data 15 kafka-input \
 1 ${CLUSTER_NAME}-w-0:9092
