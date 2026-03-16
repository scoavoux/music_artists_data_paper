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
  select(hashed_id, is_respondent, is_control)


# ---------------------------------------------------------------
# STREAMS
streams <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/new_streams/streams_all")
)

streams <- streams %>%
  filter(
    year > 2009 & year < 2025,
    is_listened == 1,
    song_id > 0 
  ) %>%
  mutate(lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  group_by(hashed_id, song_id) %>%
  summarise(
    n_play = n(),
    l_play = sum(lt),
    .groups = "drop"
  ) %>% 
  select(hashed_id, song_id, n_play, l_play)
  


# ---------------------------------------------------------------
# ATTACH USER GROUPS
# AND MAKE 2 SUBSETS (RESPONDENTS AND CONTROL)
streams <- streams %>%
  left_join(users, by = "hashed_id")

streams_control <- streams %>% 
  filter(is_control == 1)

streams_respondent <- streams %>% 
  filter(is_respondent == 1)


# ---------------------------------------------------------------
# SONG → ARTIST JOIN

streams_control <- streams_control %>%
  inner_join(song_artist_weights, by = "song_id") %>%
  mutate(
    n_play = n_play * w_feat,
    l_play = l_play * w_feat
  ) %>% 
  group_by(dz_artist_feat_id) %>%
  summarise(
    n_play_control = sum(n_play),
    l_play_control = sum(l_play),
    .groups = "drop"
  ) %>%
  collect()


# ---------------------------------------------------------------
# ARTIST POPULARITY

artist_popularity <- user_artist_groups %>%
  group_by(dz_artist_feat_id) %>%
  summarize(
    n_play_control = sum(n_play * is_control, na.rm = TRUE),
    l_play_control = sum(l_play * is_control, na.rm = TRUE),
    n_play_respondent = sum(n_play * is_respondent, na.rm = TRUE),
    l_play_respondent = sum(l_play * is_respondent, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    f_n_play_control = n_play_control / sum(n_play_control),
    f_n_play_respondent = n_play_respondent / sum(n_play_respondent),
    f_l_play_control = l_play_control / sum(l_play_control),
    f_l_play_respondent = l_play_respondent / sum(l_play_respondent)
  ) %>%
  collect()









