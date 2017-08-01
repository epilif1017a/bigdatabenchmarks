# Big Data Benchmarks [Work in progress]
Code and documents related to Big Data Benchmarks (e.g., SSB).
This repository shares shell scripts, Kafka Producers, Spark Streaming Consumers, and many other
artifacts that can be useful to evaluate Big Data systems. At the moment,
we are only concerned with an extension of the SSB benchmark, in order
to include streaming scenarios.

Currently, you will find two folders in the repository:

1) _SSB Scripts_ - contains two shell scripts with all the 13 SSB queries, using either a star schema or a flat table.
2) _ssbplusstreaming_- contains a Java Maven project with a Kafka producer and a Spark kafka consumer, processing the stream, integrating the data with the "part" dimension table of the SSB benchmark, and storing the results in two storage systems: Hive and Cassandra. [In progress]
