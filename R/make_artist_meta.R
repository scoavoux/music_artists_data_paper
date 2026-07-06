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
  
  names(dates_active)
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
      
      # add this? might be relevant
      # release_career_span = release_year_last - release_year_first
      
    )
  
  return(mbz_releases)
  
}

