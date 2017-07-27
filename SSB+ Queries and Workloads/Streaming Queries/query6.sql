-- Query 6
-- Understand the periods of the day in which Portuguese and Spanish Women tend to express more sentiments in social media towards our set of product categories.

-- Flat table version
SELECT
  CASE
    WHEN HOUR(timekey) >= 0 AND HOUR(timekey) <= 6 THEN 'Dawn'
    WHEN HOUR(timekey) > 6 AND HOUR(timekey) <= 12 THEN 'Morning'
    WHEN HOUR(timekey) > 12 AND HOUR(timekey) <= 18 THEN 'Afternoon'
    WHEN HOUR(timekey) > 18 AND HOUR(timekey) <= 23 THEN 'Night'
    else NULL END AS dayPeriod,
  p_category,
  COUNT(sentiment) as count
FROM social_part_popularity
WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
GROUP BY hour, p_category

-- Star Schema version
SELECT
  CASE
    WHEN t.hour >= 0 t.hour <= 6 THEN 'Dawn'
    WHEN t.hour > 6 AND t.hour <= 12 THEN 'Morning'
    WHEN t.hour > 12 AND t.hour <= 18 THEN 'Afternoon'
    WHEN t.hour > 18 AND t.hour <= 23 THEN 'Night'
    else NULL END AS dayPeriod,
  p.category,
  COUNT(sentiment) as count
FROM social_part_popularity AS spp
JOIN part AS p ON spp.partkey = p.partkey
JOIN time_dim AS t ON spp.partkey = t.timekey
WHERE (country = 'Portugal' OR country = 'Spain') AND gender ='Female'
GROUP BY hour, p_category