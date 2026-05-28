# compute the variables relative to releases
# first 4 input datasets are needed, which we prepare
# second, 3 of them are appended to the releases data, and some
# filtering and recodes are performed
# last, the interesting variables are calculated by grouping the
# release-level data to artist-level metrics


load_mbz_releases <- function(artists, release_file, dates_active_file, genre){
  
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
    select(dz_artist_id, genre_dz_album_1)
  
  dz_names <- artists %>% 
    filter(!is.na(mbz_artist_id)) %>% 
    select(mbz_artist_id, dz_name, dz_artist_id)
  
  # -------------------- BUILD AND CLEAN RELEASES DATASET ----------------
  release_data <- release_file %>%  
    as_tibble() %>% 
    rename(mbz_artist_id = "mbid") %>% 
    right_join(dz_names, by = c("mbz_artist_id")) %>%
    rename(dz_artist_id = "dz_artist_id.x",
           dz_name = "dz_name.x") %>% 
    left_join(dz_genre_album, by = "dz_artist_id") %>% 
  
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
    mutate(last_active_year = case_when(genre_dz_album_1 == "Classique" ~ 9999, # for composers
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