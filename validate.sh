#!/bin/bash

#export SPARK_PATH="/opt/spark"
export SPARK_MASTER_URL="spark://Ivans-MacBook-Pro.local:7077"

export CASSANDRA_HOST="localhost"
export CASSANDRA_USER="cassandra"
export CASSANDRA_PASSWORD="cassandra"

export ZK_CONNECT="localhost:2181"

export KAFKA_CONNECT="localhost:9092"
export KAFKA_SOURCE_TOPIC="dataset5"
export KAFKA_DESTINATION_TOPIC="mirror_dataset5"
export KAFKA_FETCH_SIZE="64"
export KAFKA_NUM_TOPIC_PARTITIONS="1"

eval "$SPARK_PATH/bin/spark-submit --conf spark.cassandra.connection.host=$CASSANDRA_HOST --conf spark.cassandra.auth.username=$CASSANDRA_USER --conf spark.cassandra.auth.password=$CASSANDRA_PASSWORD --executor-memory 4G --total-executor-cores 8 --class ly.stealth.shaihulud.reader.Main --master $SPARK_MASTER_URL spark-validator/build/libs/spark-validator-1.0.jar --source $KAFKA_SOURCE_TOPIC --destination $KAFKA_DESTINATION_TOPIC --partitions $KAFKA_NUM_TOPIC_PARTITIONS --zookeeper $ZK_CONNECT --broker.list $KAFKA_CONNECT --kafka.fetch.size $KAFKA_FETCH_SIZE 1> spark-validator.out 2> spark-validator.err"