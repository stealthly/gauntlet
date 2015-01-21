dataset-producer
================

Test Kafka dataset producer tool. Able to produce the given dataset to Kafka or Syslog server. Accepts following params:

`--filename`, `-f` - input file name.

`--kafka`, `-k` - Kafka broker address in `host:port` format. If this parameter is set, `--producer.config` and `--topic` must be set too (otherwise they're ignored).

`--producer.config`, `-p` - Kafka producer properties file location.

`--topic`, `-t` - Kafka topic to produce to.

`--syslog`, `-s` - Syslog server address. Format: protocol://host:port (`tcp://0.0.0.0:5140` or `udp://0.0.0.0:5141` for example)

`--loop`, `-l` - flag to loop through file until shut off manually. *False* by default.

Usage:

1. `./gradlew build`
2. `java -jar dataset-producer/build/libs/dataset-producer-*.jar --filename dataset --syslog tcp://0.0.0.0:5140 --loop true`