-- Query 5_
-- Product satisfaction analysis by uniting the top 2 more satisfied countries and the top 2 less satisfied countries.

-- Flat table
WITH sentByCountry AS (
  SELECT
    country,
    AVG(sentiment) as sent
  FROM cassandra.ssbplus.social_part_popularity_flat
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

-- Star Schema (no joins needed, therefore identical to the flat table)
WITH sentByCountry AS (
  SELECT
    country,
    AVG(sentiment) as sent
  FROM cassandra.ssbplus.social_part_popularity
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