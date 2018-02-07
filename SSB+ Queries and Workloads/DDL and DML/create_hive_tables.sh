#!/usr/bin/env bash

server=""
port="2181"
dbname=""
external=""
rowformat=""
fileformat="ORC"
location=""

for arg in "$@"
do
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            server)  server=${value} ;;
            port)    port=${value} ;;
            dbname)    dbname=${value} ;;
            external)    external=${value} ;;
            rowformat)    rowformat=${value} ;;
            fileformat)    fileformat=${value} ;;
            location)    location=${value} ;;
            -help) echo "Example Usage: create_hive_tables.sh server:example.node.com port:2181 dbname:hivedb external:EXTERNAL rowformat:\"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0059'\" fileformat:TEXTFILE location:\"LOCATION '/apps/hive/warehouse/example.db'\"
                        1) Only server and dbname are mandatory. Other variables are optional.
                        2) fileformat defaults to ORC and port defaults to 2181
                        3) all other parameters are left blank
                        INFO: This script is useful for creating the external tables where the original SSB TBL files will be uploaded, and also to create the final tables for running the workloads (typically ORC or PARQUET tables). However, you have to run the script twice, for each of these cases. If you are creating the final tables, this script also creates the flat lineorder table and the table to receive the streaming data." ;;
            *)
    esac

done

if [ "$#" -gt "2" ]
then
    beeline -u "jdbc:hive2://$server:$port/$dbname;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
    DROP DATABASE IF EXISTS $dbname CASCADE;

    CREATE DATABASE $dbname $location;

    CREATE $external TABLE $dbname.customer (custkey int, name varchar(25),
    address varchar(25), city varchar(10), nation varchar(15),
    region varchar(12), phone varchar(15), mktsegment varchar(10)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.supplier (suppkey int, name varchar(25),
    address varchar(25), city varchar(10), nation varchar(15),
    region varchar(12), phone varchar(15)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.part (partkey int, name varchar(22), mfgr varchar(6), category varchar(7),
    brand1 varchar(9), color varchar(11), type varchar(25), size int, container varchar(10)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.date_dim (datekey int, datestandard varchar(10), \`date\` varchar(18), weeknuminyear int, monthnuminyear int,
    year int, daynuminmonth int, yearmonthnum int, yearmonth varchar(7)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.time_dim (timekey varchar(4), hour int, minutes int) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.lineorder (orderkey int, linenumber int, custkey int, partkey int, suppkey int, orderdate int,
    orderpriority varchar(15), shippriority varchar(1), quantity float, extendedprice float, ordertotalprice float,
    discount float, revenue float, supplycost float, tax float, commitdate int, shipmode varchar(10)) $rowformat STORED AS $fileformat;"

   if [ ! "$external" ]
   then
       beeline -u "jdbc:hive2://$server:$port/$dbname;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
            CREATE TABLE flat_lineorder (c_custkey int, c_name varchar(25), c_address varchar(25), c_city varchar(10), c_nation varchar(15), c_region varchar(12),
            c_phone varchar(15), c_mktsegment varchar(10), s_suppkey int, s_name varchar(25), s_address varchar(25), s_city varchar(10), s_nation varchar(15),
            s_region varchar(12), s_phone varchar(15), p_partkey int, p_name varchar(22), p_mfgr varchar(6), p_category varchar(7), p_brand1 varchar(9),
            p_color varchar(11), p_type varchar(25), p_size int, p_container varchar(10), od_datekey int, od_date varchar(18), od_weeknuminyear int, od_monthnuminyear int,
            od_year int, od_daynuminmonth int, od_yearmonthnum int, od_yearmonth varchar(7), cd_datekey int, cd_date varchar(18), cd_weeknuminyear int, cd_monthnuminyear int,
            cd_year int, cd_daynuminmonth int, cd_yearmonthnum int, cd_yearmonth varchar(7), orderkey int, linenumber int, orderpriority varchar(15), shippriority varchar(1),
            quantity float, extendedprice float, ordertotalprice float, discount float, revenue float, supplycost float, tax float, shipmode varchar(10)) STORED AS $fileformat;

            CREATE EXTERNAL TABLE social_part_popularity (partkey int, datekey int, timekey varchar(4), country varchar(30), gender varchar(10), sentiment int) STORED AS $fileformat;

            CREATE EXTERNAL TABLE social_part_popularity_flat (partkey int, partcategory varchar(7), datekey int, hour int, minutes int, country varchar(30), gender varchar(10), sentiment int) STORED AS $fileformat;"

   fi

else
    echo "Example Usage: create_hive_tables.sh server:example.node.com port:2181 dbname:hivedb external:EXTERNAL rowformat:\"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0059'\" fileformat:TEXTFILE location:\"LOCATION '/apps/hive/warehouse/example.db'\"
    1) Only server and dbname are mandatory. Other variables are optional.
    2) fileformat defaults to ORC and port defaults to 2181
    3) all other parameters are left blank
    INFO: This script is useful for creating the external tables where the original SSB TBL files will be uploaded, and also to create the final tables for running the workloads (typically ORC or PARQUET tables). However, you have to run the script twice, for each of these cases. If you are creating the final tables, this script also creates the flat lineorder table and the table to receive the streaming data."
fi