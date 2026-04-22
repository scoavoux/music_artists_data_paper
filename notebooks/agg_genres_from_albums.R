library(jsonlite)

url <- "https://api.deezer.com/genre"
parsed <- fromJSON(url)

deezer_genre_mapping <- parsed$data

deezer_genre_mapping <- deezer_genre_mapping %>% 
  select(genre_id = "id", genre = "name") %>% 
  filter(genre_id != 0) %>% 
  as_tibble()

albums <- load_s3("records_w3/genres_from_albums.parquet")
  
threshold <- 0.3

artist_genre <- albums %>%
  #filter(record_type == "album") %>% 
  filter(genre_id != -1 & !is.na(genre_id)) %>% # -1 is single
  group_by(artist_id, genre_id) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(artist_id) %>%
  mutate(prop = n / sum(n)) %>%
  filter(prop >= threshold) %>%
  arrange(artist_id, desc(prop)) %>%
  slice(1) %>%  # keep first genre
  ungroup() %>% 
  select(dz_artist_id = "artist_id",
         genre_id)

artist_genre <- artist_genre %>% 
  left_join(deezer_genre_mapping, by = "genre_id") %>% 
  select(dz_artist_id, genre)


tar_load(df)

t <- df %>% 
  left_join(artist_genre, by = "dz_artist_id")

prop_na(t)










