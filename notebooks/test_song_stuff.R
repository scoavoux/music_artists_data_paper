
## feats variable



## favorites

dz_songs_sub <- dz_songs %>% 
  select(song_id, dz_artist_id)

fav <- load_s3("records_w3/favorites/RECORDS_hashed_user_favorites.parquet")

fav <- fav %>% 
  filter(item_type == "artist") %>% 
  rename(song_id = "item_id") %>% 
  select(-item_type)

fav <- fav %>% 
  inner_join(dz_songs_sub, by = "song_id")


counts <- fav %>%
  count(dz_artist_id, name = "n")


t <- df %>% 
  select(dz_artist_id, dz_name) %>% 
  inner_join(counts, by = "dz_artist_id") %>% 
  arrange(desc(n))



## aggregate at artist level



tar_load(respondent_demographics)
respondent_demographics









