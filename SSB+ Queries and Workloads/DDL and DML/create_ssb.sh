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
            -help) echo "Example usage: create_ssb.sh externaldb:myexternaldb destinationdb:mydestdb" ;;
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
else
    echo "Example usage: create_ssb.sh externaldb:myexternaldb destinationdb:mydestdb"
fi