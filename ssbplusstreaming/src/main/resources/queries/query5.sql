WITH (
  SELECT
    country,
    AVG(Sentiment) as sent
  FROM social_part_popularity
  GROUP BY country
) AS sentByCountry

SELECT *
FROM sentByCountry
ORDER BY sent DESC LIMIT 5
UNION ALL (
  SELECT *
  FROM sentByCountry
  ORDER BY sent ASC LIMIT 5
)