SELECT
  CASE
    WHEN HOUR(hourkey) >= 0 AND HOUR(hourkey) <= 6 THEN 'Dawn'
    WHEN HOUR(hourkey) > 6 AND HOUR(hourkey) <= 12 THEN 'Morning'
    WHEN HOUR(hourkey) > 12 AND HOUR(hourkey) <= 18 THEN 'Afternoon'
    WHEN HOUR(hourkey) > 18 AND HOUR(hourkey) <= 23 THEN 'Night'
    else NULL END AS dayPeriod,
  p_category,
  COUNT(sentiment) as count
FROM social_part_popularity
WHERE country = 'Portugal' AND gender ='Female'
GROUP BY hour, p_category