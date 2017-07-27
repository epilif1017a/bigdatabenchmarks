#!/usr/bin/env bash

if [ "$#" -eq "5" ]
then
    for i in 1 2 3 4
    do
        echo "***** RUN-$i *****"
        echo "***** RUN-$i *****">${4}/ss_results_hive_${5}.txt

        echo "...QUERY-1.1..."
        echo "...QUERY-1.1..." >> ${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.year = 1993 and lo.discount between 1 and 3 and lo.quantity < 25;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-1.2..."
        echo "...QUERY-1.2...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.yearmonthnum = 199401 and lo.discount between 4 and 6 and lo.quantity between 26 and 35;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-1.3..."
        echo "...QUERY-1.3...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.weeknuminyear = 6 and d.year = 1994 and lo.discount between 5 and 7 and lo.quantity between 26 and 35;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-2.1..."
        echo "...QUERY-2.1...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.category = 'MFGR#12' and s.region = 'AMERICA' group by d.year, p.brand1 order by d.year, p.brand1;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-2.2..."
        echo "...QUERY-2.2...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.brand1 between 'MFGR#2221' and 'MFGR#2228' and s.region = 'ASIA' group by d.year, p.brand1 order by d.year, p.brand1;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-2.3..."
        echo "...QUERY-2.3...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.brand1 = 'MFGR#2221' and s.region = 'EUROPE' group by d.year, p.brand1 order by d.year, p.brand1;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-3.1..."
        echo "...QUERY-3.1...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.nation, s.nation, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where c.region = 'ASIA' and s.region = 'ASIA' and d.year >= 1992 and d.year <= 1997 group by c.nation, s.nation, d.year order by d.year asc, revenue desc;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-3.2..."
        echo "...QUERY-3.2...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where c.nation = 'UNITED STATES' and s.nation = 'UNITED STATES' and d.year >= 1992 and d.year <= 1997 group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-3.3..."
        echo "...QUERY-3.3...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where (c.city='UNITED KI1' or c.city='UNITED KI5') and (s.city='UNITED KI1' or s.city='UNITED KI5') and d.year >= 1992 and d.year <= 1997 group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-3.4..."
        echo "...QUERY-3.4...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where (c.city='UNITED KI1' or c.city='UNITED KI5') and (s.city='UNITED KI1' or s.city='UNITED KI5') and d.yearmonth = 'Dec1997' group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-4.1..."
        echo "...QUERY-4.1...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, c.nation, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.region = 'AMERICA' and (p.mfgr = 'MFGR#1' or p.mfgr = 'MFGR#2') group by d.year, c.nation order by d.year, c.nation;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-4.2..."
        echo "...QUERY-4.2...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, s.nation, p.category, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.region = 'AMERICA' and (d.year = 1997 or d.year = 1998) and (p.mfgr = 'MFGR#1' or p.mfgr = 'MFGR#2') group by d.year, s.nation, p.category order by d.year, s.nation, p.category;" &>> ${4}/ss_results_hive_${5}.txt

        echo "...QUERY-4.3..."
        echo "...QUERY-4.3...">>${4}/ss_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, s.city, p.brand1, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.nation = 'UNITED STATES' and (d.year = 1997 or d.year = 1998) and p.category = 'MFGR#14' group by d.year, s.city, p.brand1 order by d.year, s.city, p.brand1;" &>> ${4}/ss_results_hive_${5}.txt

        echo "***** END-RUN-$i *****"
        echo "***** END-RUN-$i *****">>${4}/ss_results_hive_${5}.txt
    
    done

else
    echo "Example usage: ss_hive_queries.sh zookeeperserver zookeeperport databasename resultsfolderpath scalefactor.
    This uses the zookeeper connection style for Hive."
fi
