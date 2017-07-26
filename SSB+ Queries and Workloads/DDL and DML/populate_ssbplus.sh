#!/usr/bin/env bash

externaldb=""
destinationdb=""

for arg in "$@"
do
    echo ${arg}
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            sourcedb)  externaldb=${value} ;;
            destinationdb)    destinationdb=${value} ;;
            -help) echo "Example usage: populate_ssbplus.sh externaldb:myexternaldb destinationdb:mydestdb" ;;
            *)
    esac

done

if [ "$#" -eq "3" ]
then
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.customer select * from customer; analyze table $2.customer compute statistics; analyze table $2.customer compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.supplier select * from supplier; analyze table $2.supplier compute statistics; analyze table $2.supplier compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.part select * from part; analyze table $2.part compute statistics; analyze table $2.part compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.date_dim select * from date_dim; analyze table $2.date_dim compute statistics; analyze table $2.date_dim compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.time_dim select * from time_dim; analyze table $2.time_dim compute statistics; analyze table $2.time_dim compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table  $2.lineorder select * from lineorder; analyze table $2.lineorder compute statistics; analyze table $2.lineorder compute statistics for columns;"
    beeline -u "jdbc:hive2://node5.dsi.uminho.pt:2181/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "set hive.merge.tezfiles=true; insert overwrite table $2.flat_lineorder
                SELECT c.custkey as c_custkey, c.name as c_name, c.address as c_address, c.city as c_city, c.nation as c_nation, c.region as c_region, c.phone as c_phone, c.mktsegment as c_mktsegment, s.suppkey as s_suppkey, s.name as s_name, s.address as s_address, s.city as s_city, s.nation as s_nation, s.region as s_region, s.phone as s_phone, p.partkey as p_partkey, p.name as p_name, p.mfgr as p_mfgr, p.category as p_category, p.brand1 as p_brand1, p.color as p_color, p.type as p_type, p.size as p_size, p.container as p_container, od.datekey as od_datekey, od.datestandard as od_date, od.weeknuminyear as od_weeknuminyear, od.monthnuminyear as od_monthnuminyear, od.year as od_year, od.daynuminmonth as od_daynuminmonth, od.yearmonthnum as od_yearmonthnum, od.yearmonth as od_yearmonth, cd.datekey as cd_datekey, cd.datestandard as cd_date, cd.weeknuminyear as cd_weeknuminyear, cd.monthnuminyear as cd_monthnuminyear, cd.year as cd_year, cd.daynuminmonth as cd_daynuminmonth, cd.yearmonthnum as cd_yearmonthnum, cd.yearmonth as cd_yearmonth, orderkey, linenumber, orderpriority, shippriority, quantity, extendedprice, ordertotalprice, discount, revenue, supplycost, tax, shipmode
                FROM  lineorder l
                JOIN  customer c ON l.custkey = c.custkey
                JOIN  supplier s ON l.suppkey = s.suppkey
                JOIN  part p ON l.partkey = p.partkey
                JOIN  date_dim od ON l.orderdate = od.datekey
                JOIN  date_dim cd ON l.commitdate = cd.datekey; analyze table $2.flat_lineorder compute statistics; analyze table $2.flat_lineorder compute statistics for columns;"

else
    echo "Example usage: populate_ssbplus.sh externaldb:myexternaldb destinationdb:mydestdb"
fi