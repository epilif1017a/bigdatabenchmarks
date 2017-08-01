-- Query 6
-- Understand the periods of the day in which Portuguese and Spanish Women tend to express more sentiments in social media towards our set of product categories.

-- Flat table version
SELECT
  CASE
    WHEN hour >= 0 AND hour <= 6 THEN 'Dawn'
    WHEN hour > 6 AND hour <= 12 THEN 'Morning'
    WHEN hour > 12 AND hour <= 18 THEN 'Afternoon'
    WHEN hour > 18 AND hour <= 23 THEN 'Night'
    else NULL END AS dayPeriod,
  partcategory,
  COUNT(sentiment) as count
FROM cassandra.ssbplus.social_part_popularity_flat
WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
GROUP BY hour, partcategory;

-- Star Schema version
SELECT
  CASE
    WHEN t.hour >= 0 AND t.hour <= 6 THEN 'Dawn'
    WHEN t.hour > 6 AND t.hour <= 12 THEN 'Morning'
    WHEN t.hour > 12 AND t.hour <= 18 THEN 'Afternoon'
    WHEN t.hour > 18 AND t.hour <= 23 THEN 'Night'
    else NULL END AS dayPeriod,
  p.category,
  COUNT(sentiment) as count
FROM cassandra.ssbplus.social_part_popularity AS spp
JOIN hive.ssbplus300.part AS p ON spp.partkey = p.partkey
JOIN hive.ssbplus300.time_dim AS t ON spp.timekey = t.timekey
WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
GROUP BY t.hour, p.category;