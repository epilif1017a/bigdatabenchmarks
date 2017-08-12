#!/usr/bin/env bash

if [ "$#" -eq "7" ]
then
    for i in 1
    do
        echo "***** RUN-$i *****"
        echo "***** RUN-$i *****">${5}/flat_streaming_results_presto${7}_$(date select count()).txt
        echo "***** RUN-$i *****">${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "...QUERY-5-flat..."
        echo "...QUERY-5-flat..." >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-5-flat" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
            WITH sentByCountry AS (
              SELECT
                country,
                AVG(sentiment) as sent
              FROM social_part_popularity_flat
              GROUP BY country
            )

            SELECT *
            FROM (
              SELECT *
              FROM sentByCountry
              ORDER BY sent DESC LIMIT 2
            )
            UNION ALL (
              SELECT *
              FROM sentByCountry
              ORDER BY sent ASC LIMIT 2
            );
        "; } >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "...QUERY-5-star..."
        echo "...QUERY-5-star..." >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-5-star" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
            SET SESSION distributed_join=false;
            WITH sentByCountry AS (
              SELECT
                country,
                AVG(sentiment) as sent
              FROM social_part_popularity
              GROUP BY country
            )

            SELECT *
            FROM (
              SELECT *
              FROM sentByCountry
              ORDER BY sent DESC LIMIT 2
            )
            UNION ALL (
              SELECT *
              FROM sentByCountry
              ORDER BY sent ASC LIMIT 2
            );
        "; } >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt

        echo "...QUERY-6-star..."
        echo "...QUERY-6-star..." >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-6-star" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
            SET SESSION distributed_join=false;
            SELECT
              CASE
                WHEN t.hour >= 0 AND t.hour <= 6 THEN 'Dawn'
                WHEN t.hour > 6 AND t.hour <= 12 THEN 'Morning'
                WHEN t.hour > 12 AND t.hour <= 18 THEN 'Afternoon'
                WHEN t.hour > 18 AND t.hour <= 23 THEN 'Night'
                else NULL END AS dayPeriod,
              p.category,
              COUNT(sentiment) as count
            FROM social_part_popularity AS spp
            JOIN hive.$4.part AS p ON spp.partkey = p.partkey
            JOIN hive.$4.time_dim AS t ON spp.timekey = t.timekey
            WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
            GROUP BY t.hour, p.category;
        "; } >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "...QUERY-6-flat..."
        echo "...QUERY-6-flat..." >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-6-flat" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
        SELECT
          CASE
            WHEN hour >= 0 AND hour <= 6 THEN 'Dawn'
            WHEN hour > 6 AND hour <= 12 THEN 'Morning'
            WHEN hour > 12 AND hour <= 18 THEN 'Afternoon'
            WHEN hour > 18 AND hour <= 23 THEN 'Night'
            else NULL END AS dayPeriod,
          partcategory,
          COUNT(sentiment) as count
        FROM social_part_popularity_flat
        WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
        GROUP BY hour, partcategory;
        "; } >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "...QUERY-7-flat..."
        echo "...QUERY-7-flat..." >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-7-flat" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
        SELECT
          partcategory,
          gender,
          AVG(sentiment) sent
        FROM social_part_popularity_flat
        GROUP BY partcategory, gender
        HAVING AVG(sentiment) > (
                        SELECT AVG(sentiment)
                        FROM social_part_popularity_flat
                      );
        "; } >> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "...QUERY-7-star..."
        echo "...QUERY-7-star..." >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt
        { time facebookpresto --source "RUN-$i QUERY-7-star" --server ${1}:${2} --catalog ${6} --schema ${3} --execute "
            SET SESSION distributed_join=false;
            SELECT
              p.category,
              gender,
              AVG(sentiment) sent
            FROM social_part_popularity AS spp
            JOIN hive.$4.part AS p ON spp.partkey = p.partkey
            GROUP BY p.category, gender
            HAVING AVG(sentiment) > (
                            SELECT AVG(sentiment)
                            FROM social_part_popularity
                          );
        "; } >> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt 2>> ${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt


        echo "***** END-RUN-$i *****"
        echo "***** END-RUN-$i *****">>${5}/flat_streaming_results_presto${7}_$(date +%H:%M).txt
        echo "***** END-RUN-$i *****">>${5}/star_streaming_results_presto${7}_$(date +%H:%M).txt
    done

else
    echo "Example usage: presto_streaming_queries.sh server port streaming_database hive_dimensions_database results_folder_path catalog scale_factor.
    This uses the zookeeper connection style for Hive."
fi
