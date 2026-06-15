


t <- load_s3("records_w3/favorites/RECORDS_hashed_user_favorites.parquet")


function(survey_raw, dz_songs, favorites_file, dz_users){
  
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
  
  
  t <- favorites %>% 
    left_join(dz_users, by = "hashed_id") %>% 
    filter(is_control == T) %>% 
    group_by(dz_artist_id) %>%
    summarise(
      likes_n_users = n(),
      likes_n = sum(n),
    )

  # old function
  filter(item_type == "artist") %>% 
    mutate(dz_artist_id = as.character(item_id)) %>% 
    count(dz_artist_id, name = "n_favorite_artist") %>% 
    select(dz_artist_id, n_favorite_artist)
  
  
  
  song_favorites <- favorites %>% 
    
    filter(item_type == "song") %>% 
    rename(song_id = "item_id") %>% 
    
    left_join(dz_songs, by = "song_id") %>% 
    count(hashed_id, dz_artist_id) %>%
    
    inner_join(survey, by = "hashed_id") %>% # inner!
    group_by(dz_artist_id) %>%
    
    summarise(
      likes_mean_age = weighted.mean(age, w = n, na.rm = TRUE),
      likes_female_share = weighted.mean(gender, w = n, na.rm = TRUE),
      likes_higher_ed_share = weighted.mean(higher_ed, w = n, na.rm = TRUE),
      likes_graduate_ed_share = weighted.mean(graduate_ed, w = n, na.rm = TRUE),
      likes_mean_isei = weighted.mean(isei, w = n, na.rm = TRUE)
    )
  
  return(favorites)
  
}







