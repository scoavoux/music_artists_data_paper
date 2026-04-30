-- artist_genre_album_counts.sql
--
-- Output: one row per (artist MBID, genre) with the number of distinct
-- release groups (= albums) by that artist tagged with that genre.
--
-- In MusicBrainz, "genres" are not a separate annotation: they are the
-- subset of user tags whose name appears in the curated `genre` table
-- (see https://musicbrainz.org/doc/Genre). Tags can be applied to many
-- entity types; here we use `release_group_tag` because release groups
-- are the level treated as "albums" elsewhere in this project (see
-- musicbrainz_artist_language_album_counts.sql), and because genres on
-- the website are typically aggregated at the release-group level.
-- Restricted to primary type = 'Album'.
--
-- Notes on counting:
-- * A release group can carry several genre tags; it is therefore
--   counted once per genre it carries. Sums across genres for a given
--   artist will exceed that artist's total album count.
-- * `release_group_tag.count` is the net upvote count for that tag on
--   that entity (it can be <= 0 if downvotes outweigh upvotes). We keep
--   only tags with count > 0, which matches what is shown on the site.
-- * Release groups with no genre tag are dropped (no row emitted).

COPY (
WITH
  -- 1. Genre tags applied to album-type release groups, with a
  --    positive net vote.
  release_group_genre AS (
    SELECT
      rg.id  AS release_group_id,
      rg.gid AS release_group_gid,
      g.id   AS genre_id,
      g.gid  AS genre_mbid,
      g.name AS genre_name
    FROM release_group rg
    JOIN release_group_primary_type rgpt
      ON rgpt.id = rg.type
    JOIN release_group_tag rgt
      ON rgt.release_group = rg.id
    JOIN tag t
      ON t.id = rgt.tag
    JOIN genre g
      ON g.name = t.name
    WHERE rgpt.name = 'Album'
      AND rgt.count > 0
  ),

  -- 2. Map each release group to every artist it credits.
  release_group_artist AS (
    SELECT DISTINCT
      rg.id  AS release_group_id,
      a.gid  AS artist_mbid
    FROM release_group rg
    JOIN artist_credit_name acn
      ON acn.artist_credit = rg.artist_credit
    JOIN artist a
      ON a.id = acn.artist
  )

-- 3. Count release groups per (artist, genre).
SELECT
  rga.artist_mbid                          AS mbid,
  rgg.genre_name,
  COUNT(DISTINCT rgg.release_group_id)     AS n_albums
FROM release_group_artist rga
JOIN release_group_genre rgg
  ON rgg.release_group_id = rga.release_group_id
GROUP BY rga.artist_mbid, rgg.genre_name
ORDER BY rga.artist_mbid, n_albums DESC
) TO STDOUT WITH CSV HEADER;