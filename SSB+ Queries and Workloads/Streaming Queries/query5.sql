-- Query 5_
-- Product satisfaction analysis by uniting the top 2 more satisfied countries and the top 2 less satisfied countries.

-- Flat table & Star Schema version (no joins needed)
WITH (
  SELECT
    country,
    AVG(sentiment) as sent
  FROM social_part_popularity
  GROUP BY country
) AS sentByCountry

SELECT *
FROM sentByCountry
ORDER BY sent DESC LIMIT 2
UNION ALL (
  SELECT *
  FROM sentByCountry
  ORDER BY sent ASC LIMIT 2
)