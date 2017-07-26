#!/bin/bash

if [ $1 ]; then

echo 'QUERY1.1-STARTING'

echo "------------------------------------------------------------QUERY-1.1-------------------------------------------------------" > [save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.year = 1993 and lo.discount between 1 and 3 and lo.quantity < 25;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY1.1-DONE'

echo 'QUERY1.2-STARTING'

echo "------------------------------------------------------------QUERY-1.2-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.yearmonthnum = 199401 and lo.discount between 4 and 6 and lo.quantity between 26 and 35;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY1.2-DONE'

echo 'QUERY1.3-STARTING'

echo "------------------------------------------------------------QUERY-1.3-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.extendedprice*lo.discount) as revenue from lineorder lo join date_dim d on lo.orderdate = d.datekey where d.weeknuminyear = 6 and d.year = 1994 and lo.discount between 5 and 7 and lo.quantity between 26 and 35;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY1.3-DONE'

echo 'QUERY2.1-STARTING'

echo "------------------------------------------------------------QUERY-2.1-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.category = 'MFGR#12' and s.region = 'AMERICA' group by d.year, p.brand1 order by d.year, p.brand1;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY2.1-DONE'

echo 'QUERY2.2-STARTING'

echo "------------------------------------------------------------QUERY-2.2-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt


 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.brand1 between 'MFGR#2221' and 'MFGR#2228' and s.region = 'ASIA' group by d.year, p.brand1 order by d.year, p.brand1;" &>> [save_path]/results_hive_$1.txt



	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY2.2-DONE'

echo 'QUERY2.3-STARTING'

echo "------------------------------------------------------------QUERY-2.3-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(lo.revenue), d.year, p.brand1 from lineorder lo join date_dim d on lo.orderdate = d.datekey join part p on lo.partkey = p.partkey join supplier s on lo.suppkey = s.suppkey where p.brand1 = 'MFGR#2221' and s.region = 'EUROPE' group by d.year, p.brand1 order by d.year, p.brand1;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY2.3-DONE'

echo 'QUERY3.1-STARTING'

echo "------------------------------------------------------------QUERY-3.1-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.nation, s.nation, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where c.region = 'ASIA' and s.region = 'ASIA' and d.year >= 1992 and d.year <= 1997 group by c.nation, s.nation, d.year order by d.year asc, revenue desc;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY3.1-DONE'

echo 'QUERY3.2-STARTING'

echo "------------------------------------------------------------QUERY-3.2-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where c.nation = 'UNITED STATES' and s.nation = 'UNITED STATES' and d.year >= 1992 and d.year <= 1997 group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY3.2-DONE'

echo 'QUERY3.3-STARTING'

echo "------------------------------------------------------------QUERY-3.3-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where (c.city='UNITED KI1' or c.city='UNITED KI5') and (s.city='UNITED KI1' or s.city='UNITED KI5') and d.year >= 1992 and d.year <= 1997 group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY3.3-DONE'

echo 'QUERY3.4-STARTING'

echo "------------------------------------------------------------QUERY-3.4-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c.city, s.city, d.year, sum(lo.revenue) as revenue from lineorder lo join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join date_dim d on lo.orderdate = d.datekey where (c.city='UNITED KI1' or c.city='UNITED KI5') and (s.city='UNITED KI1' or s.city='UNITED KI5') and d.yearmonth = 'Dec1997' group by c.city, s.city, d.year order by d.year asc, revenue desc;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY3.4-DONE'

echo 'QUERY4.1-STARTING'

echo "------------------------------------------------------------QUERY-4.1-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, c.nation, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.region = 'AMERICA' and (p.mfgr = 'MFGR#1' or p.mfgr = 'MFGR#2') group by d.year, c.nation order by d.year, c.nation;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY4.1-DONE'

echo 'QUERY4.2-STARTING'

echo "------------------------------------------------------------QUERY-4.2-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, s.nation, p.category, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.region = 'AMERICA' and (d.year = 1997 or d.year = 1998) and (p.mfgr = 'MFGR#1' or p.mfgr = 'MFGR#2') group by d.year, s.nation, p.category order by d.year, s.nation, p.category;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY4.2-DONE'

echo 'QUERY4.3-STARTING'

echo "------------------------------------------------------------QUERY-4.3-------------------------------------------------------">>[save_path]/results_hive_$1.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_$1.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_$1.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select d.year, s.city, p.brand1, sum(lo.revenue - lo.supplycost) as profit from lineorder lo join date_dim d on lo.orderdate = d.datekey join customer c on lo.custkey = c.custkey join supplier s on lo.suppkey = s.suppkey join part p on lo.partkey = p.partkey where c.region = 'AMERICA' and s.nation = 'UNITED STATES' and (d.year = 1997 or d.year = 1998) and p.category = 'MFGR#14' group by d.year, s.city, p.brand1 order by d.year, s.city, p.brand1;" &>> [save_path]/results_hive_$1.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_$1.txt

done

echo 'QUERY4.3-DONE'

else

echo 'Missing database name!!!'

fi
