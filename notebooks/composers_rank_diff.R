df_before <- read.csv2("data/df_before_correction.csv")
df_before <- df_before %>% 
  as_tibble() %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  select(-X)

df_after <- read.csv2("data/df_with_correction.csv")
df_after <- df_after %>% 
  as_tibble() %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  select(-X)


## OPTIONAL
df_before <- df_before %>% 
  filter(genre_dz_album_1 == "Classique")

df_after <- df_after %>% 
  filter(genre_dz_album_1 == "Classique")


# Rank each dataset
df_before <- df_before %>%
  mutate(rank_before = min_rank(desc(n_plays_raw))) %>% 
  rename(n_plays_before = "n_plays_raw")

df_after <- df_after %>%
  mutate(rank_after = min_rank(desc(n_plays_raw))) %>% 
  rename(n_plays_after = "n_plays_raw")

# Join and calculate rank differences
rank_changes <- df_before %>%
  select(dz_artist_id, dz_name, rank_before, n_plays_before) %>%
  inner_join(
    df_after %>% 
      select(dz_artist_id, dz_name, rank_after, n_plays_after),
    by = "dz_artist_id"
  ) %>%
  mutate(
    rank_diff = rank_after - rank_before,
    abs_rank_diff = abs(rank_diff),
    n_plays_fct = n_plays_after / n_plays_before,
    n_plays_diff = n_plays_after - n_plays_before
  ) %>%
  arrange(desc(abs_rank_diff)) %>% 
  select("dz_name" = "dz_name.x",
         rank_before,
         rank_after,
         rank_diff,
         n_plays_fct,
         n_plays_before,
         n_plays_after,
         n_plays_diff)

rank_changes <- rank_changes %>% 
  arrange(desc(n_plays_diff))

comp <- read.csv("data/comp_wide_1706.csv",
                 sep = ";")
comp <- comp %>% 
  as_tibble()


t <- rank_changes %>% 
  #inner_join(comp, by = "dz_name") %>% 
  select(dz_name, rank_before, rank_after, rank_diff,
         n_plays_before, n_plays_after, n_plays_diff, n_plays_fct)

write.csv2(t, "data/classical_rank_diff.csv")
names(t)

t <- t %>% 
  filter(n_plays_diff != 0)


