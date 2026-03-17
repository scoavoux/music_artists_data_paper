tar_load(dz_songs)
options(arrow.skip_nul = TRUE)

# ------------
# select only relevant cols in dz_songs
song_artist_weights <- dz_songs %>%
  select(song_id, dz_artist_feat_id, w_feat)

# ----------------
# load user data
users <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")

users <- users %>% 
  mutate(is_respondent = ifelse(is_respondent == TRUE, 1, 0),
         is_control = ifelse(is_in_control_group == TRUE, 1, 0)) %>% 
  filter(is_respondent != 0 | is_control != 0) %>% # remove users who are neither
  select(hashed_id, is_respondent, is_control)


# ---------------------------------------------------------------
# STREAMS
streams <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/streams/streams_short")
)

streams <- streams %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         media_type, 
         song_id = "media_id") %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(media_type == "song", 
         is_listened == 1, 
         year == 2022,
         song_id > 0) %>% 
  inner_join(users, by = "hashed_id") %>% 
  filter(is_respondent == 1) %>% 
  group_by(song_id) %>% 
  summarize(n_play = n(),
            l_play = sum(listening_time)) %>% 
  select(song_id, n_play, l_play) %>% 
  collect()


# ---------------------------------------------------------------
# ARTIST POPULARITY
artist_popularity <- streams %>%
  inner_join(song_artist_weights, by = "song_id") %>% 
  mutate(n_play = n_play * w_feat,
         l_play = l_play * w_feat) %>% 
  group_by(dz_artist_feat_id) %>%
  summarize(
    n_play_respondent = sum(n_play),
    l_play_respondent = sum(l_play),
    .groups = "drop"
  ) %>%
  mutate(
    f_n_play_respondent = n_play_respondent / sum(n_play_respondent),
    f_l_play_respondent = l_play_respondent / sum(l_play_respondent)
  )

tar_load(all_final)

t <- all_final %>% 
  left_join(artist_popularity, by = c(dz_artist_id = "dz_artist_feat_id"))
































