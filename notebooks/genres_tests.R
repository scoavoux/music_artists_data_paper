
# --------------- SENSCRITIQUE TAGS

album_tags <- load_s3("senscritique/albums_tags.csv")
tags_meaning <- load_s3("senscritique/tags_meaning.csv")
contacts_albums_list <- load_s3("senscritique/contacts_albums_link.csv")

names(album_tags)
names(tags_meaning)
names(contacts_albums_list)




genres <- album_tags %>% 
  left_join(tags_meaning, by = "genre_tag_id") %>% 
  left_join(contacts_albums_list, by = "product_id") %>% 


  #mutate(genre = recode_vars(genre, .source)) %>% 
  filter(!is.na(contact_id), !is.na(genre)) %>%
    
  # importance of genres per artist
  count(contact_id, product_id, genre) %>% 
  mutate(f = n / sum(n), .by = product_id) %>%
  summarise(f = sum(f), .by = c(contact_id, genre)) %>% 
  mutate(f = f / sum(f), .by = contact_id) %>% 
    
  arrange(contact_id, desc(f)) %>% 
  slice(1, .by = contact_id) %>% 
  filter(f > .3) %>% 
    
  select(contact_id, genre)
  # TODO: collapse

genres <- genres %>% 
  mutate(sc_artist_id = as.character(contact_id),
         genre_sc_tags = genre) %>% 
  select(sc_artist_id, genre_sc_tags) %>% 
  as_tibble(genres)


df <- df %>% 
  left_join(genres, by = "sc_artist_id") %>% 
  select(dz_name, genre_main_dz, genre_sc_tags)

prop_na(df)



genres_dz_albums <- read_csv("data/genres_from_deezer_albums.csv")

head(genres_dz_albums)















