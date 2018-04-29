#!/usr/bin/env bash

if [ "$#" -eq "5" ]
then
    echo "***** Benchmark *****">>${4}/flat_results_hive_${5}.txt
    for i in 1 2 3 4
    do
        echo "***** RUN-$i *****"
        echo "***** RUN-$i *****">>${4}/flat_results_hive_${5}.txt

        echo "...QUERY-1.1..."
        echo "...QUERY-1.1..." >> ${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from flat_lineorder where od_year = 1993 and discount between 1 and 3 and quantity < 25;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-1.2..."
        echo "...QUERY-1.2...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from flat_lineorder where od_yearmonthnum = 199401 and discount between 4 and 6 and quantity between 26 and 35;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-1.3..."
        echo "...QUERY-1.3...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from flat_lineorder where od_weeknuminyear = 6 and od_year = 1994 and discount between 5 and 7 and quantity between 26 and 35;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-2.1..."
        echo "...QUERY-2.1...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from flat_lineorder where p_category = 'MFGR#12' and s_region = 'AMERICA' group by od_year, p_brand1 order by od_year, p_brand1;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-2.2..."
        echo "...QUERY-2.2...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from flat_lineorder where p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by od_year, p_brand1 order by od_year, p_brand1;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-2.3..."
        echo "...QUERY-2.3...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from flat_lineorder where p_brand1= 'MFGR#2239' and s_region = 'EUROPE' group by od_year, p_brand1 order by od_year, p_brand1;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-3.1..."
        echo "...QUERY-3.1...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_nation, s_nation, od_year, sum(revenue) as revenue from flat_lineorder where c_region = 'ASIA' and s_region = 'ASIA' and od_year >= 1992 and od_year <= 1997 group by c_nation, s_nation, od_year order by od_year asc, revenue desc;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-3.2..."
        echo "...QUERY-3.2...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from flat_lineorder where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and od_year >= 1992 and od_year <= 1997 group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-3.3..."
        echo "...QUERY-3.3...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from flat_lineorder where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and od_year >= 1992 and od_year <= 1997 group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-3.4..."
        echo "...QUERY-3.4...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from flat_lineorder where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and od_yearmonth = 'Dec1997' group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-4.1..."
        echo "...QUERY-4.1...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, c_nation, sum(revenue - supplycost) as profit from flat_lineorder where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by od_year, c_nation order by od_year, c_nation;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-4.2..."
        echo "...QUERY-4.2...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, s_nation, p_category, sum(revenue - supplycost) as profit from flat_lineorder where c_region = 'AMERICA' and s_region = 'AMERICA' and (od_year = 1997 or od_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by od_year, s_nation, p_category order by od_year, s_nation, p_category;" &>> ${4}/flat_results_hive_${5}.txt

        echo "...QUERY-4.3..."
        echo "...QUERY-4.3...">>${4}/flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, s_city, p_brand1, sum(revenue - supplycost) as profit from flat_lineorder where c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (od_year = 1997 or od_year = 1998) and p_category = 'MFGR#14' group by od_year, s_city, p_brand1 order by od_year, s_city, p_brand1;" &>> ${4}/flat_results_hive_${5}.txt

        echo "***** END-RUN-$i *****"
        echo "***** END-RUN-$i *****">>${4}/flat_results_hive_${5}.txt
    done

else
    echo "Example usage: flat_hive_queries.sh zookeeperserver zookeeperport databasename resultsfolderpath scalefactor.
    This uses the zookeeper connection style for Hive."
fi
