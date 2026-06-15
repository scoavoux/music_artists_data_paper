### load items_old or items_new
make_dz_songs <- function(to_remove_file, file) {
  
  to_remove <- load_s3(to_remove_file)
  
  df <- load_s3(file,
                col_select = c("song_id",
                               "artist_id",
                               "artists_ids",
                               "song_title")) %>% 
    anti_join(to_remove, 
              by = "artist_id") %>% 
    
    select(song_id,
           album_id, # ADDED 2205
           song_title,
           dz_artist_feat_id = "artists_ids",
           dz_artist_id = "artist_id")
  
  return(df)
}


bind_dz_songs <- function(dz_songs_old, dz_songs_new, classical_albums, dz_names){
  
  # bind items_old and items_new
  # prioritize deezer_id of items_new
  songs <- dz_songs_new %>% 
    bind_rows(
      dz_songs_old %>% 
        anti_join(dz_songs_new, by = "song_id")
    )
  
  # remove 3 NAs
  songs <-  songs %>% 
    filter(!is.na(dz_artist_id))
  
  # --------------------------------------------------
  # ADD CLASSICAL COMPOSERS AS EXTRA FEATURED ARTISTS
  # --------------------------------------------------

  songs <- songs %>%

    left_join(classical_albums, by = "album_id") %>%

    mutate(
      dz_artist_feat_id = map2(
        dz_artist_feat_id,
        composer_dz_artist_id,
        ~ unique(c(.x, .y))
      )
    ) %>%

    select(-composer_dz_artist_id)


  # separate rows of featurings
  songs <- songs %>% 
    mutate(dz_artist_id = map_chr(dz_artist_feat_id, # CONVERT FEAT TO ID
                                  ~ paste(as.integer(.x), 
                                          collapse = ","))) %>% 
    filter(!is.na(dz_artist_id)) %>% 
    separate_rows(dz_artist_id, sep = ",") %>% 
    select(song_id,
           album_id, # ADDED 2205
           song_title,
           dz_artist_id)
  
  ## new col to weight by n featured artists
  songs <- songs %>%
    group_by(song_id) %>%
    mutate(w_feat = 1 / n_distinct(dz_artist_id)) %>% 
    ungroup()
  
  ## add deezer names to debug joins with other ids
  songs <- songs %>% 
    left_join(dz_names, by = "dz_artist_id") 
  
  return(songs)
}


bind_dz_names <- function(file_1, file_2){
  
  names <- load_s3(file_1)
  scraped_names <- load_s3(file_2)
  
  names <- names %>% 
    mutate(dz_artist_id = as.character(artist_id), # CHANGED TO DEEZER_FEAT_ID
           dz_name = name) %>% 
    select(dz_artist_id, dz_name)
  
  scraped_names <- scraped_names %>% 
    mutate(dz_artist_id = as.character(deezer_id.new),
           dz_name = name) %>% 
    select(-c(deezer_id.new, name)) %>% 
    as_tibble()
  
  names <- names %>% 
    bind_rows(scraped_names)
  
  return(names)

}


## make alternative
compute_n_tracks <- function(dz_songs) {
  
  song_artist <- dz_songs %>%
    mutate(song_title = str_to_lower(song_title)) %>% 
    distinct(song_title, dz_artist_id)
  
  feat_info <- song_artist %>%
    mutate(song_title = str_to_lower(song_title)) %>% 
    group_by(song_title) %>%
    summarise(
      is_feat_track = n_distinct(dz_artist_id) > 1,
      .groups = "drop"
    )
  
  song_artist <- song_artist %>%
    left_join(feat_info, by = "song_title") %>%
    group_by(dz_artist_id) %>%
    summarise(
      n_tracks = n(),
      n_feat_tracks = sum(is_feat_track),
      .groups = "drop"
    )
  
  return(song_artist)
}


make_artist_favorites <- function(favorites_file, dz_songs){
  
  fav <- load_s3(favorites_file)
  
  fav_song <- fav %>% 
    filter(item_type == "song") %>% 
    inner_join(dz_songs, by = c(item_id = "song_id")) %>% 
    count(dz_artist_id, name = "n_favorite_song") %>% 
    select(dz_artist_id, n_favorite_song)
  
  fav_art <- fav %>% 
    filter(item_type == "artist") %>% 
    mutate(dz_artist_id = as.character(item_id)) %>% 
    count(dz_artist_id, name = "n_favorite_artist") %>% 
    select(dz_artist_id, n_favorite_artist)
  
  artist_favorites <- fav_song %>% 
    full_join(fav_art, by = "dz_artist_id")
  
  return(artist_favorites)
  
}


make_song_favorites <- function(survey_raw, dz_songs, favorites_file){
  
  favorites <- load_s3(favorites_file)
  
  survey <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
    mutate(gender = ifelse(E_gender == "Une femme", 1, 0)) %>% 
    mutate(age = 2023 - E_birth_year) %>% 
    left_join(raw_isei, by = "hashed_id") %>% 
    filter(E_diploma != "", !is.na(E_diploma)) %>% 
    mutate(higher_ed = ifelse(str_detect(E_diploma, "Licence|Master|Doctorat"), 1, 0),
           graduate_ed = ifelse(str_detect(E_diploma, "Master|Doctorat"), 1, 0)) %>% 
    select(hashed_id, age, gender, isei, higher_ed, graduate_ed)
  
  
  song_favorites <- favorites %>% 
    
    filter(item_type == "song") %>% 
    rename(song_id = "item_id") %>% 
    
    left_join(dz_songs, by = "song_id") %>% 
    count(hashed_id, dz_artist_id) %>%
    
    left_join(survey, by = "hashed_id") %>%
    group_by(dz_artist_id) %>%
    
    summarize(
      likes_n_users = n(),
      likes_n = sum(n),
      likes_mean_age = weighted.mean(age, w = n, na.rm = TRUE),
      likes_female_share = weighted.mean(gender, w = n, na.rm = TRUE),
      likes_higher_ed_share = weighted.mean(higher_ed, w = n, na.rm = TRUE),
      likes_graduate_ed_share = weighted.mean(graduate_ed, w = n, na.rm = TRUE),
      likes_mean_isei = weighted.mean(isei, w = n, na.rm = TRUE)
    )
  
  return(song_favorites)
  
}




















  





  