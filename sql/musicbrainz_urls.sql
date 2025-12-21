COPY (
SELECT
    musicbrainz_id,
    url
FROM(
(
    SELECT 
    entity0 as artist_id,
    entity1 as url_id
FROM l_artist_url        
) as l_artist_url
LEFT JOIN
(
    SELECT 
        id as url_id,
        url
    FROM url
) as url
ON l_artist_url.url_id = url.url_id
)
LEFT JOIN
(
    SELECT
        id as artist_id,
        gid as musicbrainz_id
    FROM artist
) as artist
ON l_artist_url.artist_id = artist.artist_id
    WHERE 
        url LIKE '%deezer%'
     OR url LIKE '%spotify%'
     OR url LIKE '%wikidata%'
     OR url LIKE '%discogs%'
     OR url LIKE '%allmusic%'
)
TO '/tmp/musicbrainz_urls.csv' WITH CSV DELIMITER ',' HEADER;
