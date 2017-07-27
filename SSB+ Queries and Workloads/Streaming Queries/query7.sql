-- Query 7
-- Analyze the product categories and gender whose average sentiment is higher than the average of the sentiments for all products in our company.

-- Flat table version
SELECT
  p_category,
  gender,
  AVG(sentiment) sent
FROM social_part_popularity
GROUP BY p_category, gender
HAVING sent > (
                SELECT AVG(sentiment)
                FROM social_part_popularity
              )

-- Star Schema version
SELECT
  p.category,
  gender,
  AVG(sentiment) sent
FROM social_part_popularity AS spp
JOIN part AS p ON spp.partkey = p.partkey
GROUP BY p.category, gender
HAVING sent > (
                SELECT AVG(sentiment)
                FROM social_part_popularity
              )