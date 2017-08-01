-- Query 7
-- Analyze the product categories and gender whose average sentiment is higher than the average of the sentiments for all products in our company.

-- Flat table version
SELECT
  partcategory,
  gender,
  AVG(sentiment) sent
FROM cassandra.ssbplus.social_part_popularity_flat
GROUP BY partcategory, gender
HAVING AVG(sentiment) > (
                SELECT AVG(sentiment)
                FROM cassandra.ssbplus.social_part_popularity_flat
              );

-- Star Schema version
SELECT
  p.category,
  gender,
  AVG(sentiment) sent
FROM cassandra.ssbplus.social_part_popularity AS spp
JOIN hive.ssb300.part AS p ON spp.partkey = p.partkey
GROUP BY p.category, gender
HAVING AVG(sentiment) > (
                SELECT AVG(sentiment)
                FROM cassandra.ssbplus.social_part_popularity
              );