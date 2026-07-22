## make artist main language from dz_songs
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



# load artists' countries from musicbrainz
# recode areas (eg cities or regions) to countries
# and rank countries by defined dict
make_artist_country <- function(mbz_area_file,
                                area_country_file,
                                country_rank_file){
  
  # AREAS TO COUNTRIES MAPPING
  area_country <- load_s3(area_country_file) %>% 
    filter(!is.na(country)) %>% 
    rename(area_name = "name") %>% 
    mutate(country = ifelse(type_name == "Country", area_name, country)) %>% 
    as_tibble() %>% 
    select(-n)
  
  # COUNTRY RANK FILE
  country_rank <- load_s3(country_rank_file) %>% 
    rename(country = "Country", rank = "Rank") %>% 
    as_tibble()
  
  mbz_artist_area <- load_s3(mbz_area_file)
  
  mbz_artist_area <- mbz_artist_area %>%
    as_tibble() %>% 
    separate(area_type, into = c("area_type_id", "area_type"),
             sep = ",",
             remove = TRUE) %>%
    mutate(
      mbz_artist_id = mbid,
      area_type_id = as.integer(gsub("[()]", "", area_type_id)),
      area_type = gsub("[()]", "", area_type)
      ) %>% 
    left_join(area_country, by = "area_name") %>% 
    distinct(mbz_artist_id, country) %>% 
    
    left_join(country_rank, by = "country") %>% 
    mutate(country = ifelse(country == "", NA, country)) %>% 
    arrange(mbz_artist_id, rank) %>% 
    group_by(mbz_artist_id) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(mbz_artist_id,
           country_of_origin = "country")
  
  
  return(mbz_artist_area)
  
}


# coalesce gender from mbz and gpt
make_artist_gender <- function(artists, mbz_gender_file, gpt_gender_file){
  
  mbz_gender <- load_s3(mbz_gender_file)
  
  mbz_gender <- mbz_gender %>% 
    rename(mbz_artist_id = "artist_mbid") %>% 
    mutate(gender = str_to_lower(gender),
           gender = ifelse(gender == "non-binary", "nonbinary", gender)) %>% 
    filter(gender %in% c("female", "male", "nonbinary")) %>% 
    as_tibble()
  
  gpt_gender <- load_s3(gpt_gender_file)
  
  gpt_gender <- gpt_gender %>% 
    mutate(dz_artist_id = as.character(artist_id)) %>% 
    filter(gender %in% c("female", "male", "nonbinary")) %>% 
    select(dz_artist_id, gender) %>%
    as_tibble()
  
  artists <- artists %>% 
    select(dz_artist_id, mbz_artist_id)
  
  mbz_gpt_gender <- artists %>% 
    
    # mbz if available
    left_join(mbz_gender, by = "mbz_artist_id") %>%
    rename(gender_mbz = gender) %>%
    
    # else gpt
    left_join(gpt_gender, by = "dz_artist_id") %>%
    rename(gender_gpt = gender) %>%
    
    mutate(
      gender = coalesce(gender_mbz, gender_gpt)
    ) %>%
    
    select(dz_artist_id, gender) %>% 
    
    # remove possible duplicates
    # ie unattributed genders
    add_count(dz_artist_id) %>% 
    filter(n == 1)

  return(mbz_gpt_gender)
}

# load raw radio plays and count per artist
count_radio_plays <- function(file){
  
  radio <- load_s3(file)
  
  radio <- radio %>%
    as_tibble() %>% 
    mutate(dz_artist_id = as.character(artist_id)) %>% 
    filter(!is.na(dz_artist_id)) %>% 
    count(dz_artist_id, radio) %>% 
    mutate(
      n_public = if_else(radio %in% c("France Musique", "France Inter", "Fip"), n, 0)
    ) %>% 
    group_by(dz_artist_id) %>% 
    summarize(radio_n_plays = sum(n),
              radio_n_plays_public_stations = sum(n_public))
  
  return(radio)
}

# compute variables related to releases
# filtering, recoding, grouping of
# release-level data and artist-level metrics
load_mbz_releases <- function(release_file, dates_active_file, genre){
  
  # -------------------- PREPARE INPUTS ----------------------
  # main release file
  release_file <- load_s3(release_file)
  
  # needed for end of collaboration
  dates_active <- load_s3(dates_active_file)
  dates_active <- dates_active %>% 
    rename(mbz_artist_id = "mbid") %>% 
    as_tibble()
  
  # changed genre 2805
  genre <- genre %>% 
    select(mbz_artist_id, genre_mbz_album_1)
  
  # -------------------- BUILD AND CLEAN RELEASES DATASET ----------------
  release_data <- release_file %>%  
    as_tibble() %>% 
    rename(mbz_artist_id = "mbid") %>% 
    left_join(genre, by = "mbz_artist_id") %>% 
    
    mutate(secondary_type_name = ifelse(secondary_type_name == "", 
                                        NA, 
                                        secondary_type_name)) %>% 
    
    # rm irrelevant release (compilations etc)
    filter(primary_type_name %in% c("Album", "EP", "Single")) %>% 
    filter(is.na(secondary_type_name)) %>% 
    
    filter(artist_position == 0) %>% # artist in 1st position only
    
    # clean first_release col
    filter(!is.na(first_release_date_year)) %>%  
    filter(first_release_date_year < 2026) %>% 
    filter(first_release_date_year > 1900) %>% 
    
    # limit release dates to end of collaboration year + 2
    left_join(dates_active, by = "mbz_artist_id") %>% 
    mutate(last_active_year = case_when(genre_mbz_album_1 == "classical" ~ 9999, # for composers
                                        is.na(end_date_year) ~ NA, # for still active artists
                                        TRUE ~ end_date_year)) %>% 
    filter(first_release_date_year < last_active_year + 2 | is.na(last_active_year)) %>% 
    
    # weight albums, singles and EPs
    group_by(mbz_artist_id, first_release_date_year) %>% 
    mutate(keep = ifelse(any(primary_type_name == "Album") & 
                           primary_type_name != "Album", 
                         FALSE, 
                         TRUE)) %>% 
    ungroup() %>% 
    filter(keep) %>% 
    mutate(weight = c("Single" = .2, "EP" = .5, "Album" = 1)[primary_type_name])
  
  
  # ------------------- COMPUTE VARIABLES --------------------------
  
  mbz_releases <- release_data %>%
    
    group_by(mbz_artist_id) %>%
    
    summarise(
      
      release_year_first = min(first_release_date_year),
      release_year_last  = max(first_release_date_year),
      
      release_count_total = sum(weight),
      
      release_count_2010_2022 = sum(weight[first_release_date_year >= 2010 &
                                             first_release_date_year <= 2022]),
      
      release_count_2019_2023 = sum(weight[first_release_date_year >= 2019 &
                                             first_release_date_year <= 2023])

    )
  
  return(mbz_releases)
  
}

