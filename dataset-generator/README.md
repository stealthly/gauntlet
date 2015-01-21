dataset-generator
================

Test Kafka dataset generation tool. Generates a random text file with given params:

`--filename`, `-f` - output file name.

`--filesize`, `-s` - desired size of output file. The actual size will always be a bit larger (with a maximum size of `$filesize + $max.length - 1`)

`--min.length`, `-l` - minimum generated entry length.

`--max.length`, `-h` - maximum generated entry length.

Usage:

1. `./gradlew build`
2. `java -jar dataset-generator/build/libs/dataset-generator-*.jar -s 100000 -l 2 -h 20`
