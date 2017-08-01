#!/usr/bin/env bash

server=""
dbname=""

for arg in "$@"
do
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            server)  server=${value} ;;
            dbname)    dbname=${value} ;;
            -help) echo "Example Usage: create_cassandra_tables.sh server:example.node.com dbname:cassandrakeyspace" ;;
            *)
    esac

done

if [ "$#" -eq "2" ]
then
    ssh admin@${server} "cqlsh $server -u presto -e \"

    drop keyspace if exists $dbname;

    create keyspace $dbname with replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

    create table $dbname.social_part_popularity (
        partkey int,
        datekey int,
        timekey text,
        country text,
        gender text,
        sentiment int,
        primary key(partkey, datekey, timekey))
        with clustering order by (datekey DESC);

    create table $dbname.social_part_popularity_flat (
        partkey int,
        partcategory text,
        datekey int,
        hour int,
        minutes int,
        country text,
        gender text,
        sentiment int,
        primary key(partkey, datekey, hour, minutes))
        with clustering order by (datekey DESC)
    \""
else
    echo "Example Usage: create_cassandra_tables.sh server:example.node.com dbname:cassandrakeyspace"
fi