#!/bin/sh
export CASSANDRA_CONNECT="localhost"
export CASSANDRA_USER="cassandra"
export CASSANDRA_PASSWORD="cassandra"

eval "java -jar validator/build/libs/validator-1.0.jar --test.id $1 --cassandra.connect $CASSANDRA_CONNECT --cassandra.user $CASSANDRA_USER --cassandra.password $CASSANDRA_PASSWORD"
