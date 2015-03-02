#!/bin/sh

./gradlew jar

export DATASET_FILE_NAME="dataset"
export DATASET_SIZE="10000000"
export DATASET_MIN_LENGTH="1024"
export DATASET_MAX_LENGTH="5000"

export PRODUCER_CONFIG="producer.properties"

export ZK_CONNECT="localhost:2181"
export KAFKA_CONNECT="localhost:9092"
export KAFKA_TOPIC="dataset"

export CLIENT_LAUNCH_COMMAND="~/gopath/src/github.com/stealthly/go_kafka_client/mirrormaker/mirrormaker --prefix mirror_ --consumer.config ~/gopath/src/github.com/stealthly/go_kafka_client/mirrormaker/consumer.config --num.streams 2 --producer.config ~/gopath/src/github.com/stealthly/go_kafka_client/mirrormaker/producer.config --whitelist=\"^$KAFKA_TOPIC\""

eval "java -jar dataset-generator/build/libs/dataset-generator-1.0.jar --filename $DATASET_FILE_NAME --filesize $DATASET_SIZE --min.length $DATASET_MIN_LENGTH --max.length $DATASET_MAX_LENGTH"
eval "java -jar dataset-producer/build/libs/dataset-producer-1.0.jar --filename $DATASET_FILE_NAME --kafka $KAFKA_CONNECT --topic $KAFKA_TOPIC --producer.config $PRODUCER_CONFIG"

eval $CLIENT_LAUNCH_COMMAND
