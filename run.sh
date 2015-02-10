#!/bin/sh

./gradlew jar

export DATASET_FILE_NAME="dataset"
export DATASET_SIZE="500000000"
export DATASET_MIN_LENGTH="1024"
export DATASET_MAX_LENGTH="5000"

export PRODUCER_CONFIG="producer.properties"

export KAFKA_CONNECT="localhost:9092"
export KAFKA_SOURCE_TOPIC="dataset"
export KAFKA_DESTINATION_TOPIC="mirror_dataset"
export KAFKA_FETCH_SIZE="64"
export KAFKA_NUM_TOPIC_PARTITIONS="1"

export ZK_CONNECT="localhost:2181"

export CLIENT_LAUNCH_COMMAND=""

export SPARK_PATH="/opt/spark"
export SPARK_MASTER_URL="spark://localhost:7077"

export CASSANDRA_HOST="localhost"
export CASSANDRA_USER="cassandra"
export CASSANDRA_PASSWORD="cassandra"

eval "java -jar dataset-generator/build/libs/dataset-generator-1.0.jar --filename $DATASET_FILE_NAME --filesize $DATASET_SIZE --min.length $DATASET_MIN_LENGTH --max.length $DATASET_MAX_LENGTH"
eval "java -jar dataset-producer/build/libs/dataset-producer-1.0.jar --filename $DATASET_FILE_NAME --kafka $KAFKA_CONNECT --topic $KAFKA_SOURCE_TOPIC --producer.config $PRODUCER_CONFIG"

eval $CLIENT_LAUNCH_COMMAND

eval "$SPARK_PATH/bin/spark-submit --conf spark.cassandra.connection.host=$CASSANDRA_HOST --conf spark.cassandra.auth.username=$CASSANDRA_USER --conf spark.cassandra.auth.password=$CASSANDRA_PASSWORD --executor-memory 4G --total-executor-cores 8 --class ly.stealth.shaihulud.reader.Main --master $SPARK_MASTER_URL spark-reader/build/libs/spark-reader-1.0.jar --source $KAFKA_SOURCE_TOPIC --destination $KAFKA_DESTINATION_TOPIC --partitions $KAFKA_NUM_TOPIC_PARTITIONS --zookeeper $ZK_CONNECT --kafka.fetch.size $KAFKA_FETCH_SIZE --testId $1 1> spark-reader.out 2> spark-reader.err &"
