#!/usr/bin/env bash

for arg in "$@"
do
    echo ${arg}
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            localpath)  localpath=${value} ;;
            hdfspath)    hdfspath=${value} ;;
            -help) echo "Example usage: upload_local_files_to_hadoop.sh localpath:/home/myuser/ssb/sf100 hdfspath:/apps/hive/warehouse/externaldb.db.
                        WARNING: Do not place '/' at the end of the paths.
                        WARNING: Date and Time dimensions should also be placed in the same local path. Remember that Time dimension does not exist in traditional SSB. Also remember that the Date dimension has been modified, in order to only include the fields used in the queries, instead of irrelevant fields in the traditional SSB. The schema is different and therefore the Date table created by our scripts is also different!!! You can use the ones provided in this repository." ;;
            *)
    esac

done

if [ "$#" -eq "3" ]
then
    echo "Uploading SSB data to HDFS"
    hdfs dfs -put ${localpath}/customer.tbl ${hdfspath}/customer
    hdfs dfs -put ${localpath}/supplier.tbl ${hdfspath}/supplier
    hdfs dfs -put ${localpath}/part.tbl ${hdfspath}/part
    hdfs dfs -put ${localpath}date_dim.csv ${hdfspath}/date_dim
    hdfs dfs -put ${localpath}time_dim.csv ${hdfspath}/time_dim
    hdfs dfs -put ${localpath}/lineorder.tbl ${hdfspath}/lineorder
    echo "Finished. Have a nice trip in Hadoop :)"
else
    echo "Example usage: upload_local_files_to_hadoop.sh localpath:/home/myuser/ssb/sf100 hdfspath:/apps/hive/warehouse/externaldb.db.
          Warning: Do not place '/' at the end of the paths.
          WARNING: Date and Time dimensions should also be placed in the same local path. Remember that Time dimension does not exist in traditional SSB. Also remember that the Date dimension has been modified, in order to only include the fields used in the queries, instead of irrelevant fields in the traditional SSB. The schema is different and therefore the Date table created by our scripts is also different!!! You can use the ones provided in this repository."
fi