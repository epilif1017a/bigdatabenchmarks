**STEP 1) DOWNLOAD A WORKING SSB GENERATOR**

Lemire's original SSB DBgen: https://github.com/lemire/StarSchemaBenchmark

If needed, a forked version is available in my Github account: https://github.com/epilif1017a/StarSchemaBenchmark

_Important:_ Remember, this SSB+ version of the benchmark uses a more streamlined date dimension, only containing the attributes that are needed in the 13 queries.
https://github.com/epilif1017a/bigdatabenchmarks/tree/master/SSB%2B%20Queries%20and%20Workloads/DDL%20and%20DML/Modified%20Date%20and%20Time%20Dimensions

**STEP 2) RUN create_hive_tables.sh (under DDL and DML folder)**

This will create the databases and the tables in your Hive DW.

This script can be used to create the initial external tables to hold the .TBL files from the SSB DBgen and also the .CSV files in this extension of the benchmark containing the dimensions new Date and Time.

_Note:_ you will find an help if you run it only with --help

**STEP 3) RUN upload_local_files_to_hadoop.sh (under DDL and DML folder)**

This will transfer the .TBL and .CSV files from a local folder to the corresponding locations of the external tables in HDFS (created with previous script).

_Note:_ help available!

STEP 4) RUN populate_ssbplus.sh (under DDL and DML folder)

This script transfers the data from the Hive external tables to the Hive final tables (typically ORC or PARQUET tables, depending on the file format you choose in the create_hive_tables.sh script).
It will also create a flat lineorder table, to compare star schemas vs. flat tables.

_Note:_ help available!

**STEP 5)** For batch scenarios (typical SSB queries on historical data) you can use any of our scripts.
These scripts contain all 13 SSB queries, for the star schema and for the flat table.
Currently, we have scripts for Hive and Presto (distributed and broadcast joins for star schema) command lines.

**STEP 6)** For streaming scenarios, run create_cassandra_table.sh, Kafka producer, and Kafka consumer to start producing data and to store it in Cassandra and Hive. Use the available streaming queries to analyze the performance of your infrastructure, depending on the Kafka Producer throughput and producing rates.
Currently, there is no autonomous way of doing it. Either use the Hive's or Presto's command line or any other interface to submit the queries. You can use any SQL-on-Hadoop engine of your choice, but be aware of the SQL compatibility level.   

**STEP 7)** Finally, a set of queries combining both batch and streaming data simultaneously are also available.
Again use any SQL-on-Hadoop engine and interface of your choice.