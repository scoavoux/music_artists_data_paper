-- artist_language_album_counts.sql
--
-- Output: one row per (artist MBID, language ISO code) with the number of
-- distinct release groups (= albums) by that artist in that language.
--
-- Language is stored on the `release` (edition); we aggregate to the
-- release group by taking the modal language across its releases.
-- Release groups with no language information on any release are dropped.
-- Restricted to primary type = 'Album'.

COPY (
    WITH
    -- 1. Pick one language per release group: the most frequent language
    --    across its releases, ties broken by lower language id (stable).
    release_group_language AS (
        SELECT
            rg.id   AS release_group_id,
            rg.gid  AS release_group_gid,
            l.id    AS language_id,
            l.iso_code_3 AS language_iso,
            l.name  AS language_name
        FROM release_group rg
        JOIN release_group_primary_type rgpt
          ON rgpt.id = rg.type
        JOIN LATERAL (
            SELECT r.language AS language_id,
                   COUNT(*)   AS n
            FROM   release r
            WHERE  r.release_group = rg.id
              AND  r.language IS NOT NULL
            GROUP BY r.language
            ORDER BY n DESC, r.language ASC
            LIMIT 1
        ) modal ON TRUE
        JOIN language l
          ON l.id = modal.language_id
        WHERE rgpt.name = 'Album'
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

    -- 3. Count release groups per (artist, language).
    SELECT
        rga.artist_mbid AS mbid,
        rgl.language_iso,
        rgl.language_name,
        COUNT(DISTINCT rgl.release_group_id) AS n_albums
    FROM release_group_artist rga
    JOIN release_group_language rgl
      ON rgl.release_group_id = rga.release_group_id
    GROUP BY rga.artist_mbid, rgl.language_iso, rgl.language_name
    ORDER BY rga.artist_mbid, n_albums DESC

) TO STDOUT WITH CSV HEADER;