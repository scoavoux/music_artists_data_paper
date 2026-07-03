library(dplyr)


tar_load(dz_songs)
tar_load(dz_users)


# ---------------------------------------------------------------
# STREAMS (CONTROL GROUP ONLY)
# ---------------------------------------------------------------

streams <- query_raw_streams(
  path_long = "records_w3/streams/streams_long",
  path_short = "records_w3/streams/streams_short",
  dz_songs = dz_songs,
  dz_users = dz_users
) %>%
  filter(is_respondent == 0)

# ---------------------------------------------------------------
# SONG-LEVEL POPULARITY
# ---------------------------------------------------------------

song_popularity <- streams %>%
  group_by(song_title_norm, dz_artist_id) %>% # changed to song_title
  summarise(
    n_plays_raw = n(),
    n_plays = sum(w_feat),
    n_users = n_distinct(hashed_id),
    .groups = "drop"
  ) %>%
  collect()

# ---------------------------------------------------------------
# SONG SHARE WITHIN ARTIST
# ---------------------------------------------------------------

song_popularity <- song_popularity %>%
  group_by(dz_artist_id) %>%
  mutate(
    n_plays_share = n_plays_raw / sum(n_plays_raw)
  ) %>%
  ungroup()

# ---------------------------------------------------------------
# ARTIST CONCENTRATION / INEQUALITY
# ---------------------------------------------------------------

artist_concentration <- song_popularity %>%
  group_by(dz_artist_id) %>%
  arrange(desc(n_plays_raw), .by_group = TRUE) %>%
  mutate(
    rank = row_number(),
    cum_share = cumsum(n_plays_share)
  ) %>%
  summarise(
    
    # concentration
    div_top1_share = first(n_plays_share),
    div_top5_share = sum(head(n_plays_share, 5)),
    div_top10_share = sum(head(n_plays_share, 10)),
    
    # h-index
    div_h_index = {
      x <- sort(n_users, decreasing = TRUE)
      max(c(0, which(x >= seq_along(x))))
    },
    
    # Shannon effective diversity
    div_shannon_effective = prod(n_plays_share^n_plays_share)^(-1),
    
    # Songs required for 80% of listening
    div_songs_for_80 = which(cum_share >= .8)[1],
    
    .groups = "drop"
  )


# names <- df %>% 
#   select(dz_artist_id, dz_name)
# 
# t <- names %>% 
#   left_join(artist_concentration, by = "dz_artist_id") %>% 
#   select(dz_artist_id, dz_name, starts_with("div_"))
# 








