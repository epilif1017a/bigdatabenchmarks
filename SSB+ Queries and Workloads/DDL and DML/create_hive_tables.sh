#!/usr/bin/env bash

server=""
port="10000"
dbname=""
external=""
rowformat=""
fileformat="ORC"

for arg in "$@"
do
    echo ${arg}
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            server)  server=${value} ;;
            port)    port=${value} ;;
            dbname)    dbname=${value} ;;
            external)    external=${value} ;;
            rowformat)    rowformat=${value} ;;
            fileformat)    fileformat=${value} ;;
            -help) echo "Example Usage: create_hive_tables.sh server:example.node.com port:10000 dbname:hivedb external:EXTERNAL rowformat:'DELIMITED FIELDS TERMINATED BY ;' fileformat:TEXTFILE
                        1) Only server and dbname are mandatory. Other variables are optional.
                        2) fileformat defaults to ORC and port defaults to 10000
                        3) all other parameters are left blank" ;;
            *)
    esac

done

if [ "$#" -eq "3" ]
then
    beeline
    -u "jdbc:hive2://$server:$port/$dbname;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
    -e "
    DROP DATABASE IF EXISTS $dbname CASCADE;

    CREATE DATABASE $dbname;

    CREATE $external TABLE $dbname.customer(custkey int, name varchar(25),
    address varchar(25), city varchar(10), nation varchar(15),
    region varchar(12), phone varchar(15), mktsegment varchar(10)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.supplier(suppkey int, name varchar(25),
    address varchar(25), city varchar(10), nation varchar(15),
    region varchar(12), phone varchar(15)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.part(partkey int, name varchar(22), mfgr varchar(6), category varchar(7),
    brand1 varchar(9), color varchar(11), type varchar(25), size int, container varchar(10)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.date_dim(datekey int, datestandard varchar(10), date varchar(18), weeknuminyear int, monthnuminyear int,
    year int, daynuminmonth int, yearmonthnum int, yearmonth varchar(7)) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.time_dim(timekey int, hour int, minutes int) $rowformat STORED AS $fileformat;

    CREATE $external TABLE $dbname.lineorder (orderkey int, linenumber int, custkey int, partkey int, suppkey int, orderdate int,
    orderpriority varchar(15), shippriority varchar(1), quantity float, extendedprice float, ordertotalprice float,
    discount float, revenue float, supplycost float, tax float, commitdate int, shipmode varchar(10)) $rowformat STORED AS $fileformat
"
else
    echo "Example Usage: create_hive_tables.sh server:example.node.com port:10000 dbname:hivedb external:EXTERNAL rowformat:'DELIMITED FIELDS TERMINATED BY ;' fileformat:TEXTFILE
    1) Only server and dbname are mandatory. Other variables are optional.
    2) fileformat defaults to ORC and port defaults to 10000
    3) all other parameters are left blank"
fi