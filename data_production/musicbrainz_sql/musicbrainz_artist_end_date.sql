COPY (
  SELECT 
  gid as mbid,
  end_date_year
  FROM artist
  WHERE end_date_year IS NOT NULL
)
TO STDOUT WITH CSV DELIMITER ',' HEADER;