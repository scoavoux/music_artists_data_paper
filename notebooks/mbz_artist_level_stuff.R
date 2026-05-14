library(sjmisc)
library(janitor)
library(stringr)
library(ggplot2)

# ------- 1. load genres and inner_join to df

tar_load(df)

genres_raw <- load_s3("musicbrainz/musicbrainz_artist_releasegroup_genre.csv")


t <- load_s3("musicbrainz/musicbrainz_artist_genre.csv")
head(t)
frq(genres_raw$genre_votes, sort.frq = "desc")


### remove live artists --- few cases only
genres <- genres_raw %>% 
  as_tibble() %>% 
  rename(mbz_artist_id = "artist_mbid") %>% 
  filter(genre_name != "") %>% 
  select(-album_type)

dat <- df %>% 
  inner_join(genres, by = "mbz_artist_id") %>% 
  select(mbz_artist_id, dz_name, genre_name, n_plays_share, n_releases)

genres_artist <- dat %>% 
  group_by(mbz_artist_id, dz_name, genre_name, n_plays_share) %>%
  summarise(
    n_releases = sum(n_releases),
    .groups = "drop"
  ) %>%
  filter(!is.na(genre_name)) %>%
  group_by(mbz_artist_id, dz_name, n_plays_share) %>%
  arrange(desc(n_releases), .by_group = TRUE) %>%
  slice_head(n = 2) %>%   # keep top 2 genres only
  mutate(rank = row_number()) %>%
  select(-n_releases) %>%
  tidyr::pivot_wider(
    names_from = rank,
    values_from = genre_name,
    names_prefix = "mbz_genre_"
  ) %>%
  ungroup() %>% 
  arrange(desc(n_plays_share))

frq(genres_artist$mbz_genre_1, sort.frq = "desc")

### filter irrelevant genres
genres_artist_relevant <- genres_artist %>% 
  group_by(mbz_genre_1) %>% 
  add_count(mbz_genre_1) %>% 
  filter(n > 10) %>% 
  ungroup() %>% 
  select(-n)

d <- frq(genres_artist_relevant$mbz_genre_1, sort.frq = "desc")[[1]]

write.csv2(d, "data/mbz_genre_frq_min10.csv", sep = ";")


# ------- 2. recode and aggregate genres