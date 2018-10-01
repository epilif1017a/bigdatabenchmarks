#!/usr/bin/env bash

if [ "$#" -eq "5" ]
then
    echo "***** Benchmark *****">${4}/daa_flat_results_hive_${5}.txt
    for i in 1 2 3 4
    do
        echo "***** RUN-$i *****"
        echo "***** RUN-$i *****">>${4}/daa_flat_results_hive_${5}.txt

        echo "...QUERY-5..."
        echo "...QUERY-5..." >> ${4}/daa_flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "SELECT s_name, SUM(quantity) as quantity FROM flat_lineorder l WHERE od_year IN (1999, 1998, 1997, 1996) AND EXISTS (SELECT r.s_suppkey FROM flat_returns r WHERE r.c_region = 'AMERICA' AND rd_year IN (1999, 1998, 1997, 1996) AND l.s_suppkey = r.s_suppkey) GROUP BY s_name ORDER BY quantity DESC LIMIT 20;" &>> ${4}/daa_flat_results_hive_${5}.txt

        echo "...QUERY-6.1..."
        echo "...QUERY-6.1..." >> ${4}/daa_flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "WITH returns_found AS (SELECT p_partkey as partkey, count(1) as count_returns FROM flat_returns r WHERE p_mfgr = 'MFGR#3' AND s_region = 'ASIA' AND c_region = 'AMERICA' GROUP BY p_partkey) SELECT p_name, p_category, count(1) as count FROM flat_lineorder l WHERE p_mfgr = 'MFGR#3' AND s_region = 'ASIA' AND c_region = 'AMERICA' AND EXISTS (SELECT partkey FROM returns_found rf WHERE l.p_partkey = rf.partkey AND count_returns > 1) GROUP BY p_name, p_category HAVING AVG(extendedprice) > 1000;" &>> ${4}/daa_flat_results_hive_${5}.txt

        echo "...QUERY-6.2..."
        echo "...QUERY-6.2..." >> ${4}/daa_flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "WITH returns_found AS (SELECT p_partkey as partkey, count(1) as count_returns FROM flat_returns r GROUP BY p_partkey) SELECT p_name, p_category, count(1) as count FROM flat_lineorder l WHERE EXISTS (SELECT partkey FROM returns_found rf WHERE l.p_partkey = rf.partkey AND count_returns > 1) GROUP BY p_name, p_category HAVING AVG(extendedprice) > 1000 ORDER BY count DESC LIMIT 10;" &>> ${4}/daa_flat_results_hive_${5}.txt


        echo "...QUERY-7..."
        echo "...QUERY-7..." >> ${4}/daa_flat_results_hive_${5}.txt
        beeline -u "jdbc:hive2://$1:$2/$3;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "WITH ranks AS (SELECT part_name, c_mktsegment, revenue, RANK() OVER (PARTITION BY c_mktsegment ORDER BY revenue DESC) as rank FROM (SELECT p_name as part_name, c_mktsegment, SUM(revenue) as revenue FROM flat_lineorder GROUP BY p_name, c_mktsegment) as part_mkt_rev) SELECT * FROM ranks WHERE rank <= 5;" &>> ${4}/daa_flat_results_hive_${5}.txt

        echo "***** END-RUN-$i *****"
        echo "***** END-RUN-$i *****">>${4}/daa_flat_results_hive_${5}.txt
    done

else
    echo "Example usage: daa_flat_hive_queries.sh zookeeperserver zookeeperport databasename resultsfolderpath scalefactor.
    This uses the zookeeper connection style for Hive."
fi
