#!/bin/bash

export SPARK_PATH="/opt/spark"
export MESOS_MASTER_URL="mesos://localhost:5050"
export MESOS_EXECUTOR_URI="https://dist.apache.org/repos/dist/release/spark/spark-1.2.1/spark-1.2.1-bin-cdh4.tgz"
export MESOS_COARSE_GRAINED="true"

export CASSANDRA_HOST="localhost"
export CASSANDRA_USER="cassandra"
export CASSANDRA_PASSWORD="cassandra"

export ZK_CONNECT="localhost:2181"

export KAFKA_CONNECT="localhost:9092"
export KAFKA_SOURCE_TOPIC="dataset"
export KAFKA_DESTINATION_TOPIC="mirror_dataset"
export KAFKA_FETCH_SIZE="64"
export KAFKA_NUM_TOPIC_PARTITIONS="1"

while [[ $# > 1 ]]
do
key="$1"

case $key in
    --spark.path)
    SPARK_PATH="$2"
    shift
    ;;
    --cassandra.host)
    CASSANDRA_HOST="$2"
    shift
    ;;
    --cassandra.user)
    CASSANDRA_USER="$2"
    shift
    ;;
    --cassandra.password)
    CASSANDRA_PASSWORD="$2"
    shift
    ;;
    --zk.connect)
    ZK_CONNECT="$2"
    shift
    ;;
    --kafka.connect)
    KAFKA_CONNECT="$2"
    shift
    ;;
    --kafka.source.topic)
    KAFKA_SOURCE_TOPIC="$2"
    shift
    ;;
    --kafka.destination.topic)
    KAFKA_DESTINATION_TOPIC="$2"
    shift
    ;;
    --kafka.fetch.size)
    KAFKA_FETCH_SIZE="$2"
    shift
    ;;
    --kafka.partitions)
    KAFKA_NUM_TOPIC_PARTITIONS="$2"
    shift
    ;;
    --mesos.executor.uri)
    MESOS_EXECUTOR_URI="$2"
    shift
    ;;
    --mesos.coarseGrained)
    MESOS_COARSE_GRAINED="$2"
    shift
    ;;
    --mesos.master)
    MESOS_MASTER_URL="$2"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
shift
done

eval "$SPARK_PATH/bin/spark-submit --conf spark.cassandra.connection.host=$CASSANDRA_HOST --conf spark.cassandra.auth.username=$CASSANDRA_USER --conf spark.cassandra.auth.password=$CASSANDRA_PASSWORD --executor-memory 4G --total-executor-cores 8 --class ly.stealth.shaihulud.reader.Main --master $MESOS_MASTER_URL spark-validator/build/libs/spark-validator-1.0.jar --source $KAFKA_SOURCE_TOPIC --destination $KAFKA_DESTINATION_TOPIC --partitions $KAFKA_NUM_TOPIC_PARTITIONS --zookeeper $ZK_CONNECT --broker.list $KAFKA_CONNECT --kafka.fetch.size $KAFKA_FETCH_SIZE --executor.uri $MESOS_EXECUTOR_URI --mesos.coarse $MESOS_COARSE_GRAINED --avro false 1> spark-validator.out 2> spark-validator.err"