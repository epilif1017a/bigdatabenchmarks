# Big Data Benchmarks [Work in progress]
Code and documents related to Big Data Benchmarks (e.g., SSB).
This repository shares shell scripts, Kafka Producers, Spark Streaming Consumers, and many other
artifacts that can be useful to evaluate Big Data systems. At the moment,
we are only concerned with an extension of the SSB benchmark, in order
to include streaming scenarios.

Currently, you will find two folders in the repository:

1) _SSB+ Queries and Workloads_ - contains queries and shell scripts to create the tables in Hive and Cassandra. It also contains the scripts to query the system.
2) _ssbplusstreaming_- contains a Java Maven project with a Kafka producer and a Spark kafka consumer, processing the stream, integrating the data with the "part" dimension table of the SSB benchmark, and storing the results in two storage systems: Hive and Cassandra. [In progress]
