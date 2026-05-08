-- musicbrainz_artist_release_group_genre.sql
--
-- Output: one row per (artist, release group, genre) triple, written to
-- stdout as CSV. Columns:
--
--   release_group_mbid   MBID of the release group ("album").
--   artist_mbid          MBID of an artist credited on that release group.
--   n_releases           Number of releases (editions/pressings/formats)
--                        within the release group. A weak proxy for
--                        commercial reissue activity; not a measure of
--                        cultural importance — see caveats below.
--   album_type           'Studio' or 'Live'. Studio means primary type
--                        Album with no secondary types; Live means
--                        primary type Album with the 'Live' secondary
--                        type (and no other secondary type).
--   genre_name           Name of a genre tag on the release group, or
--                        NULL if the release group carries no genre
--                        tags. Release groups with several genres
--                        produce one row per genre.
--
-- Scope and filters:
-- * Only release groups whose primary type is 'Album'. Singles, EPs,
--   Broadcasts, Other are excluded.
-- * Among Albums, only those with no secondary type ('Studio') or with
--   'Live' as the only secondary type are kept. Compilations,
--   Soundtracks, Remixes, Demos, DJ-mixes, Mixtape/Street, Audio drama,
--   Spokenword, Interview, Field recording, and any combination
--   involving these are dropped — including 'Live + Compilation'.
-- * Only release groups credited to fewer than 3 artists
--   (`artist_credit.artist_count < 3`). This keeps solo and duo
--   credits and excludes large-collaboration release groups (tribute
--   albums, "Various Artists" compilations that slipped past the
--   secondary-type filter, etc.).
-- * Genre tags are the subset of user tags whose name appears in the
--   curated `genre` table (https://musicbrainz.org/doc/Genre). Only
--   tags with a positive net vote (`release_group_tag.count > 0`) are
--   kept; tags with non-positive net score are dropped.
-- * Genres are taken from `release_group_tag`, i.e. tags applied
--   directly to the release group. They are NOT aggregated from the
--   artist's `artist_tag` rows nor from the underlying releases.
--
-- Row multiplicity (important for any aggregation downstream):
-- * A release group with k credited artists (k < 3) and g genre tags
--   produces k * max(g, 1) rows.
-- * A release group with no genre tags appears once per credited
--   artist with `genre_name = NULL` (LEFT JOIN to the genre CTE).
-- * Counting distinct release_group_mbid per artist gives the artist's
--   album count under this scope; summing n_releases across rows will
--   overcount because of the artist and genre fanout.
--
-- Caveats:
-- * "Studio" is not a formal MusicBrainz category; it is reconstructed
--   here as "Album with no secondary type". Editorial completeness of
--   secondary types is uneven, so some live albums lacking the 'Live'
--   tag will be misclassified as Studio.
-- * Filtering out non-Live secondary types removes Compilation,
--   Soundtrack, etc. This biases the output by genre: rock and pop
--   discographies are well represented, but jazz, classical, and
--   electronic catalogues — where compilations, soundtracks, mixtapes,
--   and DJ-mixes are primary artistic outputs — are under-represented.
-- * `n_releases` reflects how often the release group has been issued
--   (across countries, formats, remasters). It is correlated with
--   commercial reach but confounded with album age and label size.
--   MusicBrainz stores no popularity, sales, chart, or streaming data;
--   any popularity proxy must come from another source.
-- * Duo albums appear twice (once per credited artist). Joint
--   collaborative albums are therefore double-counted at the
--   (artist, release_group) level; this is intentional for
--   artist-level analysis but should be remembered when totalling.
COPY (
WITH
  -- 1. Studio/Live album release groups: primary type = Album,
  --    no secondary types other than (optionally) 'Live'.
  --    Compute the album_type and the number of releases per group.
  rg_filtered AS (
    SELECT
      rg.id                       AS release_group_id,
      rg.gid                      AS release_group_mbid,
      rg.artist_credit            AS artist_credit,
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM release_group_secondary_type_join rgstj
          JOIN release_group_secondary_type rgst
            ON rgst.id = rgstj.secondary_type
          WHERE rgstj.release_group = rg.id
            AND rgst.name = 'Live'
        ) THEN 'Live'
        ELSE 'Studio'
      END                         AS album_type,
      (SELECT COUNT(*) FROM release r WHERE r.release_group = rg.id)
                                  AS n_releases
    FROM release_group rg
    JOIN release_group_primary_type rgpt
      ON rgpt.id = rg.type
    WHERE rgpt.name = 'Album'
      AND NOT EXISTS (
        SELECT 1
        FROM release_group_secondary_type_join rgstj2
        JOIN release_group_secondary_type rgst2
          ON rgst2.id = rgstj2.secondary_type
        WHERE rgstj2.release_group = rg.id
          AND rgst2.name <> 'Live'
      )
  ),

  -- 2. Map each release group to every credited artist, but only keep
  --    release groups credited to fewer than 3 artists.
  rg_artist AS (
    SELECT
      rgf.release_group_id,
      rgf.release_group_mbid,
      rgf.album_type,
      rgf.n_releases,
      a.gid AS artist_mbid
    FROM rg_filtered rgf
    JOIN artist_credit ac
      ON ac.id = rgf.artist_credit
    JOIN artist_credit_name acn
      ON acn.artist_credit = ac.id
    JOIN artist a
      ON a.id = acn.artist
    WHERE ac.artist_count < 3
  ),

  -- 3. Genre tags on each release group, with positive net vote.
  rg_genre AS (
    SELECT DISTINCT
      rgt.release_group AS release_group_id,
      g.name            AS genre_name
    FROM release_group_tag rgt
    JOIN tag t
      ON t.id = rgt.tag
    JOIN genre g
      ON g.name = t.name
    WHERE rgt.count > 0
  )

-- 4. Final output: one row per (artist, release group, genre).
--    LEFT JOIN keeps release groups with no genre tags (genre_name NULL).
SELECT
  rga.release_group_mbid,
  rga.artist_mbid,
  rga.n_releases,
  rga.album_type,
  rgg.genre_name
FROM rg_artist rga
LEFT JOIN rg_genre rgg
  ON rgg.release_group_id = rga.release_group_id
ORDER BY rga.artist_mbid, rga.release_group_mbid, rgg.genre_name
) TO STDOUT WITH CSV HEADER;