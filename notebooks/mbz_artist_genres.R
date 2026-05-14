


mbz_artist_genre <- load_s3("musicbrainz/musicbrainz_artist_genre.csv")

mbz_artist_genre <- mbz_artist_genre %>% 
  as_tibble() %>% 
  rename(mbz_name = "name",
         mbz_artist_id = "artist_mbid")

dat <- df %>% 
  inner_join(mbz_artist_genre, by = "mbz_artist_id") %>% 
  select(mbz_artist_id, dz_name, genre_name, n_plays_share, genre_count)

### import recoded genres
genre_recode <- read.csv("data/mbz_genre_frq-2.csv", sep = ";")
genre_recode <- genre_recode %>% 
  select(orig_genre, new_genre) %>% 
  as_tibble()

### apply recodes from csv
dat <- dat %>% 
  left_join(genre_recode, by = c(genre_name = "orig_genre")) %>%
  mutate(genre_name = new_genre) %>% 
  select(-c(new_genre, n))
dat

genres_artist <- dat %>% 
  group_by(mbz_artist_id, dz_name, genre_name, n_plays_share) %>%
  summarise(
    genre_count = sum(genre_count),
    .groups = "drop"
  ) %>%
  filter(!is.na(genre_name)) %>%
  group_by(mbz_artist_id, dz_name, n_plays_share) %>%
  filter(genre_count == max(genre_count)) %>% 
  select(-genre_count) %>%
  ungroup() %>% 
  arrange(desc(n_plays_share))


genres_artist <- genres_artist %>% 
  #group_by(mbz_artist_id) %>% 
  add_count(mbz_artist_id, name = "n") %>% 
  ungroup() %>% 
  rename(artist_genre = "genre_name")


check <- check %>% 
  select(mbz_artist_id, mbz_genre_1)

genres_artist_check <- genres_artist %>% 
  inner_join(check, by = "mbz_artist_id") %>% 
  select(mbz_artist_id, dz_name, mbz_genre_1,
         artist_genre, n_plays_share)

cor.test(genres_artist_check$mbz_genre_1,
         genres_artist_check$artist_genre)

genres_artist_check %>%

  count(artist_genre, mbz_genre_1) %>%
  group_by(artist_genre) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup() %>%
  ggplot(aes(x = artist_genre, y = mbz_genre_1, fill = pct)) +
  geom_tile(color = "white") +
  geom_text(
    aes(label = scales::percent(pct, accuracy = 1)),
    size = 3
  ) +
  scale_fill_viridis_c(
    limits = c(0, 1),
    labels = scales::percent
  ) +
  coord_equal() +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid = element_blank()
  ) +
  labs(
    x = "artist level",
    y = "album lebel",
    fill = "% within artist level"
  )












