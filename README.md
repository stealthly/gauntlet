# gauntlet
Apache Kafka Test Framework for Producer and Consumers for Compatibility Testing

#Prerequisties
 - Spark binaries
 - Mesos master
 - Kafka
 - Zookeeper
 - Cassandra

#Note
This package is intended to use under Mesos only.

#Usage example:
```bash
./gradlew jar
./run.sh --name dataset --size 10000000 --min.length 1024 --max.length 5000 --producer.config producer.properties --zk.connect localhost:2181 --kafka.connect localhost:9092 --kafka.topic dataset --client.runner "./run.client"
./validate.sh --spark.path /opt/spark --cassandra.host localhost --cassandra.user cassandra --cassandra.password cassandra --zk.connect localhost:2181 --kafka.connect localhost:9092 --kafka.source.topic dataset --kafka.destination.topic mirror_dataset --kafka.fetch.size 64 --kafka.partitions 1 --mesos.executor.uri https://dist.apache.org/repos/dist/release/spark/spark-1.2.1/spark-1.2.1-bin-cdh4.tgz --mesos.coarseGrained true --mesos.master mesos://localhost:5050
```