COPY (
    SELECT
        artist.musicbrainz_id,
        artist.artist_name,
        url.url
    FROM
        (
            SELECT 
                entity0 AS artist_id,
                entity1 AS url_id
            FROM l_artist_url        
        ) AS l_artist_url
    LEFT JOIN
        (
            SELECT 
                id AS url_id,
                url
            FROM url
        ) AS url
        ON l_artist_url.url_id = url.url_id
    LEFT JOIN
        (
            SELECT
                id AS artist_id,
                gid AS musicbrainz_id,
                name AS artist_name
            FROM artist
        ) AS artist
        ON l_artist_url.artist_id = artist.artist_id
    WHERE 
           url.url LIKE '%deezer%'
        OR url.url LIKE '%spotify%'
        OR url.url LIKE '%wikidata%'
        OR url.url LIKE '%discogs%'
        OR url.url LIKE '%allmusic%'
)
TO STDOUT
WITH CSV DELIMITER ',' HEADER;
