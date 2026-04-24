
library(ggplot2)

### how many albums/other releases do artists have?

release_data <- albums %>% 
  #filter(record_type == "album") %>% 
  group_by(dz_artist_id) %>% 
  summarise(
    n_genres = n_distinct(genre_id),
    n_releases = n(),
    .groups = "drop"
  )

df_release <- df %>% 
  left_join(release_data, by = "dz_artist_id") %>% 
  select(dz_name, dz_artist_id, n_genres, n_releases, n_plays_share)
  

t <- df_release %>% 
  filter(n_releases < 5)

t <- albums %>% 
  filter(dz_artist_id == 808)


### how many genres do artists have, and what part do non-albums
### play in it?

artist_genre_counts <- albums %>%
  group_by(artist_id) %>%
  summarise(
    n_genres = n_distinct(genre_id),
    .groups = "drop"
  )

artist_genre_counts %>%
  summarise(avg_genres = mean(n_genres, na.rm = TRUE))

t <- df %>% 
  left_join(artist_genre_counts, by = c(dz_artist_id = "artist_id")) %>% 
  select(dz_name, dz_artist_id, n_genres)


### how dominant is the dominant one among artists with several genres?

albums_multi_genre <- albums %>%
  group_by(artist_id, genre_id) %>%
  summarise(n = n(), .groups = "drop") %>%   # count releases per genre
  group_by(artist_id) %>%
  mutate(frac_genre = n / sum(n))            # share within artist


t <- albums_multi_genre %>% 
  group_by(artist_id) %>% 
  filter(frac_genre > 0.3)

test <- df %>% 
  anti_join(t, by = c(dz_artist_id = "artist_id")) %>% 
  select(dz_name, dz_artist_id, n_plays_share)

sum(test$n_plays_share)





















