#!/usr/bin/env bash

server=""
port="2181"
externaldb=""
destinationdb=""

for arg in "$@"
do
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            server)  server=${value} ;;
            port)    port=${value} ;;
            externaldb)  externaldb=${value} ;;
            destinationdb)    destinationdb=${value} ;;
            -help) echo "Example usage: move_from_ext_to_final_tables.sh server:server_name port:port_number externaldb:myexternaldb destinationdb:mydestdb" ;;
            *)
    esac

done

if [ "$#" -gt "2" ]
then
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.customer select * from customer; alter table $destinationdb.customer concatenate; analyze table $destinationdb.customer compute statistics; analyze table $destinationdb.customer compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.supplier select * from supplier; alter table $destinationdb.supplier concatenate; analyze table $destinationdb.supplier compute statistics; analyze table $destinationdb.supplier compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.part select * from part; alter table $destinationdb.part concatenate; analyze table $destinationdb.part compute statistics; analyze table $destinationdb.part compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.date_dim select * from date_dim; alter table $destinationdb.date_dim concatenate;analyze table $destinationdb.date_dim compute statistics; analyze table $destinationdb.date_dim compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.time_dim select * from time_dim; alter table $destinationdb.time_dim concatenate; analyze table $destinationdb.time_dim compute statistics; analyze table $destinationdb.time_dim compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table  $destinationdb.lineorder select * from lineorder; alter table $destinationdb.lineorder concatenate; analyze table $destinationdb.lineorder compute statistics; analyze table $destinationdb.lineorder compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$destinationdb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "insert overwrite table $destinationdb.flat_lineorder
                SELECT c.custkey as c_custkey, c.name as c_name, c.address as c_address, c.city as c_city, c.nation as c_nation, c.region as c_region, c.phone as c_phone, c.mktsegment as c_mktsegment, s.suppkey as s_suppkey, s.name as s_name, s.address as s_address, s.city as s_city, s.nation as s_nation, s.region as s_region, s.phone as s_phone, p.partkey as p_partkey, p.name as p_name, p.mfgr as p_mfgr, p.category as p_category, p.brand1 as p_brand1, p.color as p_color, p.type as p_type, p.size as p_size, p.container as p_container, od.datekey as od_datekey, od.datestandard as od_date, od.weeknuminyear as od_weeknuminyear, od.monthnuminyear as od_monthnuminyear, od.year as od_year, od.daynuminmonth as od_daynuminmonth, od.yearmonthnum as od_yearmonthnum, od.yearmonth as od_yearmonth, cd.datekey as cd_datekey, cd.datestandard as cd_date, cd.weeknuminyear as cd_weeknuminyear, cd.monthnuminyear as cd_monthnuminyear, cd.year as cd_year, cd.daynuminmonth as cd_daynuminmonth, cd.yearmonthnum as cd_yearmonthnum, cd.yearmonth as cd_yearmonth, orderkey, linenumber, orderpriority, shippriority, quantity, extendedprice, ordertotalprice, discount, revenue, supplycost, tax, shipmode
                FROM  lineorder l
                JOIN  customer c ON l.custkey = c.custkey
                JOIN  supplier s ON l.suppkey = s.suppkey
                JOIN  part p ON l.partkey = p.partkey
                JOIN  date_dim od ON l.orderdate = od.datekey
                JOIN  date_dim cd ON l.commitdate = cd.datekey; alter table flat_lineorder concatenate; analyze table flat_lineorder compute statistics; analyze table flat_lineorder compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
                CREATE TABLE $destinationdb.returns STORED AS ORC AS
                    SELECT orderkey, linenumber, custkey, partkey, suppkey, orderdate, DATE_FORMAT( DATE_ADD( FROM_UNIXTIME( UNIX_TIMESTAMP( CAST(orderdate AS STRING), 'yyyyMMdd' ) ), CAST(round(rand() * 15) AS INT) ), 'yyyyMMdd') as returndate, round(rand() * quantity, 1) as returnedquantity
                    FROM $destinationdb.lineorder
                    WHERE rand() > 0.75; alter table $destinationdb.returns concatenate; analyze table $destinationdb.returns compute statistics; analyze table $destinationdb.returns compute statistics for columns;"
    beeline -u "jdbc:hive2://$server:$port/$externaldb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
                CREATE TABLE $destinationdb.flat_returns STORED AS ORC AS SELECT c.custkey as c_custkey, c.name as c_name, c.address as c_address, c.city as c_city, c.nation as c_nation, c.region as c_region, c.phone as c_phone, c.mktsegment as c_mktsegment, s.suppkey as s_suppkey, s.name as s_name, s.address as s_address, s.city as s_city, s.nation as s_nation, s.region as s_region, s.phone as s_phone, p.partkey as p_partkey, p.name as p_name, p.mfgr as p_mfgr, p.category as p_category, p.brand1 as p_brand1, p.color as p_color, p.type as p_type, p.size as p_size, p.container as p_container, rd.datekey as rd_datekey, rd.datestandard as rd_date, rd.weeknuminyear as rd_weeknuminyear, rd.monthnuminyear as rd_monthnuminyear, rd.year as rd_year, rd.daynuminmonth as rd_daynuminmonth, rd.yearmonthnum as rd_yearmonthnum, rd.yearmonth as rd_yearmont, orderkey, linenumber, returnedquantity
                FROM  $destinationdb.returns l
                JOIN  $destinationdb.customer c ON l.custkey = c.custkey
                JOIN  $destinationdb.supplier s ON l.suppkey = s.suppkey
                JOIN  $destinationdb.part p ON l.partkey = p.partkey
                JOIN  $destinationdb.date_dim rd ON l.returndate = rd.datekey; alter table $destinationdb.flat_returns concatenate; analyze table $destinationdb.flat_returns compute statistics; analyze table $destinationdb.flat_returns compute statistics for columns;"

else
    echo "Example usage: move_from_ext_to_final_tables.sh server:server_name port:port_number externaldb:myexternaldb destinationdb:mydestdb"
fi