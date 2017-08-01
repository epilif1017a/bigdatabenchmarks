#!/usr/bin/env bash

for arg in "$@"
do
    key=$(echo ${arg} | cut -f 1 -d :)
    value=$(echo ${arg} | cut -f 2 -d :)

    case "$key" in
            scalefactor)    scalefactor=${value} ;;
            dbgenpath)    dbgenpath=${value} ;;
            hdfspath)    hdfspath=${value} ;;
            -help) echo "Example usage: gen_files_and_sendto_hadoop.sh scalefactor:100 dbgenpath:/mypath/ hdfspath:/apps/hive/warehouse/externaldb.db.
                        WARNING: Do not place '/' at the end of the paths.
                        WARNING: Date and Time dimensions should also be placed in the same dbgen path. Remember that Time dimension does not exist in traditional SSB. Also remember that the Date dimension has been modified, in order to only include the fields used in the queries, instead of irrelevant fields in the traditional SSB. The schema is different and therefore the Date table created by our scripts is also different!!! You can use the ones provided in this repository." ;;
            *)
    esac

done

if [ "$#" -eq "3" ]
then
    echo "Generating SSB data"
    cd ${dbgenpath}
    ./dbgen -T c -s ${scalefactor}
    ./dbgen -T p -s ${scalefactor}
    ./dbgen -T s -s ${scalefactor}
    ./dbgen -T l -s ${scalefactor}

    echo "Uploading SSB data to HDFS"
    hdfs dfs -put -f ${dbgenpath}/customer.tbl ${hdfspath}/customer
    hdfs dfs -put -f ${dbgenpath}/supplier.tbl ${hdfspath}/supplier
    hdfs dfs -put -f ${dbgenpath}/part.tbl ${hdfspath}/part
    hdfs dfs -put -f ${dbgenpath}/date_dim.csv ${hdfspath}/date_dim
    hdfs dfs -put -f ${dbgenpath}/time_dim.csv ${hdfspath}/time_dim
    hdfs dfs -put -f ${dbgenpath}/lineorder.tbl ${hdfspath}/lineorder
    echo "Finished. Have a nice trip in Hadoop :)"
else
    echo "Example usage: gen_files_and_sendto_hadoop.sh scalefactor:100 dbgenpath:/mypath/ hdfspath:/apps/hive/warehouse/externaldb.db.
          Warning: Do not place '/' at the end of the paths.
          WARNING: Date and Time dimensions should also be placed in the same dbgen path. Remember that Time dimension does not exist in traditional SSB. Also remember that the Date dimension has been modified, in order to only include the fields used in the queries, instead of irrelevant fields in the traditional SSB. The schema is different and therefore the Date table created by our scripts is also different!!! You can use the ones provided in this repository."
fi