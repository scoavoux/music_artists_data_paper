SELECT 
  a.gid AS artist_mbid,
  a.name,
  g.name AS genre_name, 
  at.count AS genre_count
FROM artist a
JOIN artist_tag at ON at.artist = a.id
JOIN tag t         ON t.id = at.tag
JOIN genre g       ON g.name = t.name
WHERE at.count > 0;