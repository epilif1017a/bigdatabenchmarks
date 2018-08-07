#!/usr/bin/env bash

if [ "$#" -eq "5" ]
then
    echo "***** Benchmark *****">${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt
    for i in 1 2 3 4
    do
        echo "***** RUN-$i *****"
        echo "***** RUN-$i *****">>${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

        echo "...QUERY-8..."
        echo "...QUERY-8..." >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt
        { time facebookpresto --source "RUN-$i Q1.1" --server ${1}:${2} --catalog hive --schema ${3} --execute "SET SESSION distributed_join=false; SELECT s.name, SUM(quantity) as quantity FROM lineorder l JOIN supplier s ON l.suppkey = s.suppkey JOIN date_dim d ON l.orderdate = d.datekey WHERE d.year IN (1999, 1998, 1997, 1996) AND EXISTS (SELECT suppkey FROM returns r JOIN customer c2 ON r.custkey = c2.custkey JOIN date_dim d2 ON r.returndate = d2.datekey WHERE c2.region = 'AMERICA' AND d2.year IN (1999, 1998, 1997, 1996) AND l.suppkey = r.suppkey) GROUP BY s.name ORDER BY quantity DESC LIMIT 20;"; } >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt 2>> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

        echo "...QUERY-9.1..."
        echo "...QUERY-9.1..." >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt
        { time facebookpresto --source "RUN-$i Q1.1" --server ${1}:${2} --catalog hive --schema ${3} --execute "SET SESSION distributed_join=false; WITH returns_found AS (SELECT r.partkey as partkey, count(1) as count_returns FROM returns r JOIN part p ON r.partkey = p.partkey JOIN customer c ON r.custkey = c.custkey JOIN supplier s ON r.suppkey = s.suppkey WHERE p.mfgr = 'MFGR#3' AND s.region = 'ASIA' AND c.region = 'AMERICA' GROUP BY r.partkey) SELECT p.name, p.category, count(1) as count FROM lineorder l JOIN part p ON l.partkey = p.partkey JOIN customer c ON l.custkey = c.custkey JOIN supplier s ON l.suppkey = s.suppkey WHERE p.mfgr = 'MFGR#3' AND s.region = 'ASIA' AND c.region = 'AMERICA' AND EXISTS (SELECT partkey FROM returns_found rf WHERE l.partkey = rf.partkey AND count_returns > 1) GROUP BY p.name, p.category HAVING AVG(extendedprice) > 1000;"; } >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt 2>> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

        echo "...QUERY-9.2..."
        echo "...QUERY-9.2..." >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt
        { time facebookpresto --source "RUN-$i Q1.1" --server ${1}:${2} --catalog hive --schema ${3} --execute "SET SESSION distributed_join=false; WITH returns_found AS (SELECT r.partkey as partkey, count(1) as count_returns FROM returns r GROUP BY r.partkey) SELECT p.name, p.category, count(1) as count FROM lineorder l JOIN part p ON l.partkey = p.partkey WHERE EXISTS (SELECT partkey FROM returns_found rf WHERE l.partkey = rf.partkey AND count_returns > 1) GROUP BY p.name, p.category HAVING AVG(extendedprice) > 1000 ORDER BY count DESC LIMIT 10;"; } >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt 2>> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

        echo "...QUERY-10..."
        echo "...QUERY-10..." >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt
        { time facebookpresto --source "RUN-$i Q1.1" --server ${1}:${2} --catalog hive --schema ${3} --execute "SET SESSION distributed_join=false; WITH ranks AS (SELECT part_name, mktsegment, revenue, RANK() OVER (PARTITION BY mktsegment ORDER BY revenue DESC) as rank FROM (SELECT p.name as part_name, mktsegment, SUM(revenue) as revenue FROM lineorder l JOIN customer c ON l.custkey = c.custkey JOIN part p ON l.partkey = p.partkey GROUP BY p.name, mktsegment) as part_mkt_rev) SELECT * FROM ranks WHERE rank <= 5;"; } >> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt 2>> ${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

        echo "***** END-RUN-$i *****"
        echo "***** END-RUN-$i *****">>${4}/daa_ss_bcastjoins_results_facebookpresto${5}.txt

    done

else
    echo "Example usage: ss_bcastjoins_presto_queries.sh server port databasename resultsfolderpath scalefactor.
    This uses the zookeeper connection style for Hive."
fi
