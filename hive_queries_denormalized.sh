#!/bin/bash

re='^[0-9]+$'
if [ $1 ] && [ $2 ] && [[ $2 =~ $re ]]; then

echo 'QUERY1.1-STARTING'

echo "------------------------------------------------------------QUERY-1.1-------------------------------------------------------" > [save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from analytical_obj$2 where od_year = 1993 and discount between 1 and 3 and quantity < 25;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY1.1-DONE'

echo 'QUERY1.2-STARTING'

echo "------------------------------------------------------------QUERY-1.2-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from analytical_obj$2 where od_yearmonthnum = 199401 and discount between 4 and 6 and quantity between 26 and 35;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY1.2-DONE'

echo 'QUERY1.3-STARTING'

echo "------------------------------------------------------------QUERY-1.3-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(extendedprice*discount) as revenue from analytical_obj$2 where od_weeknuminyear = 6 and od_year = 1994 and discount between 5 and 7 and quantity between 26 and 35;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY1.3-DONE'

echo 'QUERY2.1-STARTING'

echo "------------------------------------------------------------QUERY-2.1-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from analytical_obj$2 where p_category = 'MFGR#12' and s_region = 'AMERICA' group by od_year, p_brand1 order by od_year, p_brand1;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY2.1-DONE'

echo 'QUERY2.2-STARTING'

echo "------------------------------------------------------------QUERY-2.2-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt


 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from analytical_obj$2 where p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by od_year, p_brand1 order by od_year, p_brand1;" &>> [save_path]/results_hive_d$2.txt



	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY2.2-DONE'

echo 'QUERY2.3-STARTING'

echo "------------------------------------------------------------QUERY-2.3-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select sum(revenue), od_year, p_brand1 from analytical_obj$2 where p_brand1= 'MFGR#2239' and s_region = 'EUROPE' group by od_year, p_brand1 order by od_year, p_brand1;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY2.3-DONE'

echo 'QUERY3.1-STARTING'

echo "------------------------------------------------------------QUERY-3.1-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_nation, s_nation, od_year, sum(revenue) as revenue from analytical_obj$2 where c_region = 'ASIA' and s_region = 'ASIA' and od_year >= 1992 and od_year <= 1997 group by c_nation, s_nation, od_year order by od_year asc, revenue desc;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY3.1-DONE'

echo 'QUERY3.2-STARTING'

echo "------------------------------------------------------------QUERY-3.2-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from analytical_obj$2 where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and od_year >= 1992 and od_year <= 1997 group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY3.2-DONE'

echo 'QUERY3.3-STARTING'

echo "------------------------------------------------------------QUERY-3.3-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from analytical_obj$2 where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and od_year >= 1992 and od_year <= 1997 group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY3.3-DONE'

echo 'QUERY3.4-STARTING'

echo "------------------------------------------------------------QUERY-3.4-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select c_city, s_city, od_year, sum(revenue) as revenue from analytical_obj$2 where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and od_yearmonth = 'Dec1997' group by c_city, s_city, od_year order by od_year asc, revenue desc;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY3.4-DONE'

echo 'QUERY4.1-STARTING'

echo "------------------------------------------------------------QUERY-4.1-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, c_nation, sum(revenue - supplycost) as profit from analytical_obj$2 where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by od_year, c_nation order by od_year, c_nation;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY4.1-DONE'

echo 'QUERY4.2-STARTING'

echo "------------------------------------------------------------QUERY-4.2-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, s_nation, p_category, sum(revenue - supplycost) as profit from analytical_obj$2 where c_region = 'AMERICA' and s_region = 'AMERICA' and (od_year = 1997 or od_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by od_year, s_nation, p_category order by od_year, s_nation, p_category;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY4.2-DONE'

echo 'QUERY4.3-STARTING'

echo "------------------------------------------------------------QUERY-4.3-------------------------------------------------------">>[save_path]/results_hive_d$2.txt

for i in 1 2 3 4

do 

        echo "-------------------------------------------------------------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

	echo "--------------------------------------------RUN-"$i"---------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

 	beeline -u "jdbc:hive2://[server_name]:[port]/$1;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "select od_year, s_city, p_brand1, sum(revenue - supplycost) as profit from analytical_obj$2 where c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (od_year = 1997 or od_year = 1998) and p_category = 'MFGR#14' group by od_year, s_city, p_brand1 order by od_year, s_city, p_brand1;" &>> [save_path]/results_hive_d$2.txt

	echo "-------------------------------------------FIM-RUN-"$i"------------------------------------------------------------">>[save_path]/results_hive_d$2.txt

done

echo 'QUERY4.3-DONE'

else

echo 'Missing database and/or SF number!!! (SF number is an integer)'

fi
