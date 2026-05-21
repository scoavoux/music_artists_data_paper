library(sjmisc)

tar_load(mbz_genre_artist)

# with overall genre frequency col "frq"

# ------------------ COMPARE WITH ALBUMS

tar_load(mbz_genre_album)

mbz_genre_album <- mbz_genre_album %>% 
  left_join(mbz_genre_artist_frq, by = c(mbz_genre = "val"))

mbz_genre_album <- mbz_genre_album %>% 
  group_by(mbz_artist_id) %>% 
  add_count(mbz_genre) %>% 
  filter(n == max(n)) %>% 
  filter(frq == max(frq)) %>% 
  ungroup() %>% 
  distinct(mbz_artist_id, dz_name, mbz_genre, n_plays_share, n)


a <- mbz_genre_album %>% 
  select(mbz_artist_id, dz_name, album_genre = "mbz_genre") %>% 
  group_by(album_genre) %>% 
  add_count(album_genre) %>% 
  filter(n > 20) %>% 
  ungroup() %>% 
  select(-n)

b <- mbz_genre_artist %>% 
  select(mbz_artist_id, artist_genre = "mbz_genre") %>% 
  group_by(artist_genre) %>% 
  add_count(artist_genre) %>% 
  filter(n > 20) %>% 
  ungroup() %>% 
  select(-n)


artist_v_album <- a %>% 
  inner_join(b, by = "mbz_artist_id")


# ------------------- heatmap

artist_v_album %>%
  count(album_genre, artist_genre) %>%
  group_by(album_genre) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup() %>%
  ggplot(aes(x = album_genre, y = artist_genre, fill = pct)) +
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
    x = "album_genre",
    y = "artist_genre",
    fill = "% within album genre"
  )






















