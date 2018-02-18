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
    beeline -u "jdbc:hive2://$server:$port/$destinationdb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
    create table orders STORED AS ORC as SELECT orderkey, min(custkey) as custkey, min(orderdate) as orderdate, min(orderpriority) as orderpriority, sum(quantity) as quantity, sum(revenue) as revenue, count(1) as numproducts FROM lineorder group by orderkey; alter table orders concatenate; analyze table orders compute statistics; analyze table orders compute statistics for columns;
    "

    beeline -u "jdbc:hive2://$server:$port/$destinationdb;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
    create table flat_orders STORED AS ORC as SELECT orderkey, c.custkey as c_custkey, c.name as c_name, c.address as c_address, c.city as c_city, c.nation as c_nation, c.region as c_region, c.phone as c_phone, c.mktsegment as c_mktsegment, od.datekey as od_datekey, od.datestandard as od_date, od.weeknuminyear as od_weeknuminyear, od.monthnuminyear as od_monthnuminyear, od.year as od_year, od.daynuminmonth as od_daynuminmonth, od.yearmonthnum as od_yearmonthnum, od.yearmonth as od_yearmonth, orderpriority, quantity, revenue, numproducts FROM orders o join customer c on o.custkey = c.custkey join date_dim od on o.orderdate = od.datekey; alter table flat_orders concatenate; analyze table flat_orders compute statistics; analyze table flat_orders compute statistics for columns;
    "

else
    echo "Example usage: move_from_ext_to_final_tables.sh server:server_name port:port_number externaldb:myexternaldb destinationdb:mydestdb"
fi