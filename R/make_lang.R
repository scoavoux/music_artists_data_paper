make_lang <- function(dz_songs,
                      con        = NULL,
                      simulation = SIMULATION,
                      local_dir  = LOCAL_DATA_DIR) {
  
  # ---- resolve sources: local files in sim, s3:// otherwise ------------------
  song_lang_src <- if (simulation)
    file.path(local_dir, "records_w3/items/song_id_lang.csv")
  else
    "s3://scoavoux/records_w3/items/song_id_lang.csv"
  
  deezer_src <- if (simulation)
    file.path(local_dir, "interim/prod/artists_songs_languages.csv")
  else
    "s3://scoavoux/interim/prod/artists_songs_languages.csv"
  
  # ---- connection ------------------------------------------------------------
  if (is.null(con)) {
    con <- if (simulation) DBI::dbConnect(duckdb::duckdb()) else duckdb_s3_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  }
  
  # register dz_songs (only the 2 needed cols) as a zero-copy virtual table
  duckdb::duckdb_register(
    con, "dz_songs",
    dplyr::transmute(dz_songs,
                     song_id      = as.character(song_id),      # bit64 -> exact digit string
                     dz_artist_id = as.character(dz_artist_id))
  )
  on.exit(duckdb::duckdb_unregister(con, "dz_songs"), add = TRUE)
  
  sql <- sprintf(r"(
    WITH exploded AS (
      SELECT song_id,
             unnest(string_split(regexp_replace(lang, '[\[\]"]', '', 'g'), ',')) AS lang
      FROM read_csv_auto('%s')
    ),
    song_lang AS (
      SELECT song_id, trim(lang) AS lang
      FROM exploded
      WHERE trim(lang) NOT IN ('', 'zxx', 'qlt-instrumental')
    ),
    counts AS (
      SELECT s.dz_artist_id, l.lang, COUNT(*) AS n
      FROM dz_songs s
      JOIN song_lang l ON s.song_id = CAST(l.song_id AS VARCHAR)   -- text = text
      GROUP BY s.dz_artist_id, l.lang
    ),
    tracks_top AS (
      SELECT dz_artist_id,
             lang         AS language_main,
             n            AS language_main_n_songs,
             total_tracks AS language_main_n_total_songs
      FROM (
        SELECT dz_artist_id, lang, n,
               SUM(n)       OVER (PARTITION BY dz_artist_id)                 AS total_tracks,
               ROW_NUMBER() OVER (PARTITION BY dz_artist_id ORDER BY n DESC) AS rk
        FROM counts
      )
      WHERE rk = 1
    ),
    deezer_top AS (
      SELECT dz_artist_id, language_main, language_main_n_songs
      FROM (
        SELECT CAST(art_id AS VARCHAR) AS dz_artist_id,
               lang                    AS language_main,
               nb_songs                AS language_main_n_songs,
               ROW_NUMBER() OVER (PARTITION BY CAST(art_id AS VARCHAR)
                                  ORDER BY nb_songs DESC) AS rk
        FROM read_csv_auto('%s')
      )
      WHERE rk = 1
    )
    SELECT * FROM tracks_top
    UNION ALL
    SELECT d.dz_artist_id, d.language_main, d.language_main_n_songs,
           CAST(NULL AS BIGINT) AS language_main_n_total_songs
    FROM deezer_top d
    WHERE NOT EXISTS (SELECT 1 FROM tracks_top t WHERE t.dz_artist_id = d.dz_artist_id)
  )", song_lang_src, deezer_src)
  
  tibble::as_tibble(DBI::dbGetQuery(con, sql))
}