artist_genre_release <- albums %>%
  group_by(artist_id, genre) %>%
  summarise(n_release = n(), .groups = "drop") %>%
  group_by(artist_id) %>%
  mutate(prop_release = n_release / sum(n_release)) %>%
  arrange(artist_id, desc(prop_release)) %>%
  slice(1) %>%
  ungroup() %>%
  select(
    dz_artist_id = artist_id,
    genre_release = genre
  )


artist_genre_fans <- albums %>%
  group_by(artist_id, genre) %>%
  summarise(
    fans_sum = sum(fans, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(artist_id) %>%
  mutate(prop_fans = fans_sum / sum(fans_sum)) %>%
  arrange(artist_id, desc(prop_fans)) %>%
  slice(1) %>%
  ungroup() %>%
  select(
    dz_artist_id = artist_id,
    genre_fans = genre
  )


n_early <- 3

artist_genre_early <- albums %>%
  arrange(artist_id, release_date) %>%
  group_by(artist_id) %>%
  slice_head(n = n_early) %>%
  ungroup() %>%
  group_by(artist_id, genre) %>%
  summarise(n_early_release = n(), .groups = "drop") %>%
  group_by(artist_id) %>%
  mutate(prop_early = n_early_release / sum(n_early_release)) %>%
  arrange(artist_id, desc(prop_early)) %>%
  slice(1) %>%
  ungroup() %>%
  select(
    dz_artist_id = artist_id,
    genre_early = genre
  )


t <- df %>%
  left_join(artist_genre_release, by = "dz_artist_id") %>%
  left_join(artist_genre_fans,    by = "dz_artist_id") %>%
  left_join(artist_genre_early,   by = "dz_artist_id") %>%
  select(
    dz_name,
    n_plays_share,
    genre_release,
    genre_fans,
    genre_early,
    genre_dz_main
  )



t %>%
  summarise(
    release_vs_fans  = mean(genre_release == genre_fans, na.rm = TRUE),
    release_vs_early = mean(genre_release == genre_early, na.rm = TRUE),
    fans_vs_early    = mean(genre_fans == genre_early, na.rm = TRUE)
  )


disagree <- t %>%
  filter(
    genre_release != genre_fans |
      genre_release != genre_early
  )


disagree_500 <- disagree %>% 
  select(-starts_with("prop_")) %>% 
  slice(1:500)


write.table(disagree_500, "data/genre_disagree_500.csv", sep = ";")










