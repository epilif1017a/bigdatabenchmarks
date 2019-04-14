# Big Data Benchmarks
Code and documents related to Big Data Benchmarks (e.g., SSB).
This repository shares shell scripts, Kafka Producers, Spark Streaming Consumers, and many other
artifacts that can be useful to evaluate Big Data systems. At the moment,
we are only concerned with an extension of the SSB benchmark, in order
to include streaming scenarios.

Currently, you will find two folders in the repository:

1) _SSB+ Queries and Workloads_ - contains queries and shell scripts to create the tables in Hive and Cassandra. It also contains the scripts to query the system.
2) _ssbplusstreaming_- contains a Java Maven project with a Kafka producer and a Spark kafka consumer, processing the stream, integrating the data with the "part" dimension table of the SSB benchmark, and storing the results in two storage systems: Hive and Cassandra. [In progress]

**Instructions:**

**STEP 1) DOWNLOAD A WORKING SSB GENERATOR**

Lemire's original SSB DBgen: https://github.com/lemire/StarSchemaBenchmark

If needed, a forked version is available in my Github account: https://github.com/epilif1017a/StarSchemaBenchmark

_Important:_ Remember, this SSB+ version of the benchmark uses a more streamlined date dimension, only containing the attributes that are needed in the 13 queries.
https://github.com/epilif1017a/bigdatabenchmarks/blob/master/ssbplus_queries_and_workloads/ddl_and_dml/StarSchemaBenchmark/date_dim.csv

**STEP 2) RUN create_hive_tables.sh (under DDL and DML folder)**

This will create the databases and the tables in your Hive DW.

This script can be used to create the initial external tables to hold the .TBL files from the SSB DBgen and also the .CSV files in this extension of the benchmark containing the dimensions new Date and Time.

_Note:_ you will find an help if you run it only with --help

**STEP 2) RUN create_cassandra_tables.sh (under DDL and DML folder)**

This will create the databases and the tables in your cassandra to hold the streaming data.
Currently only the cassandra script is available. However, feel free to adapt it to other shells (e.g., HBase shell).

_Note:_ you will find an help if you run it only with --help

**STEP 3) RUN gen_files_and_sendto_hadoop.sh (under DDL and DML folder)**

This will run the dbgen to generate the SSB data and transfer the .TBL and .CSV files from a local folder to the corresponding locations of the external tables in HDFS (created with previous script).

_Note:_ help available!

**STEP 4) RUN move_from_ext_to_final_tables.sh (under DDL and DML folder)**

This script transfers the data from the Hive external tables to the Hive final tables (typically ORC or PARQUET tables, depending on the file format you choose in the create_hive_tables.sh script).
It will also create a flat lineorder table, to compare star schemas vs. flat tables.

_Note:_ help available!

**STEP 5)** For batch scenarios (typical SSB queries on historical data +  drill accross and window queries) you can use any of our scripts.
These scripts contain all 13 SSB queries (+ 4 new queries), for the star schema and for the flat table.
Currently, we have scripts for Hive and Presto (distributed and broadcast joins for star schema) command lines.

**STEP 6)** For streaming scenarios, run create_cassandra_table.sh, Kafka producer, and Kafka consumer to start producing data and to store it in Cassandra and Hive. Use the available streaming queries to analyze the performance of your infrastructure, depending on the Kafka Producer throughput and producing rates.
Currently, there is no autonomous way of doing it. Either use the Hive's or Presto's command line or any other interface to submit the queries. You can use any SQL-on-Hadoop engine of your choice, but be aware of the SQL compatibility level.   
