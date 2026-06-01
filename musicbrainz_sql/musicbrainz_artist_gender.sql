-- musicbrainz_artist_gender.sql
--
-- Output: one row per artist with a recorded gender, written to stdout
-- as CSV. Columns:
--
--   artist_mbid   MBID of the artist.
--   gender        Gender label as stored in MusicBrainz
--                 (Male, Female, Other, Not applicable).

COPY (
SELECT
  a.gid  AS artist_mbid,
  g.name AS gender
FROM artist a
JOIN gender g
  ON g.id = a.gender
ORDER BY a.gid
) TO STDOUT WITH CSV HEADER;