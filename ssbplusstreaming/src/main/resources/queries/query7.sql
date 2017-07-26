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