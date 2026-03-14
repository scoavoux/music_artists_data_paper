tar_load(dz_songs)
tar_load(streams)

options(arrow.skip_nul = TRUE)

## need to change how i load streams: user-artist level instead
## of song level and compute all popularity variables from this
## maybe agg to song to compute dz_stream_share after this step
## or just replace it with control_f_n_play


## conceptually:

#### 1. make the 2 queries for streams, with 1 row <=>  


library(dplyr)




# ----------------
song_artist_weights <- dz_songs %>%
  select(song_id, dz_artist_feat_id, w_feat)
# ----------------

# ---------------------------------------------------------------

data_cloud <- arrow::open_dataset(
  arrow::s3_bucket(
    "scoavoux",
    endpoint_override = "minio.lab.sspcloud.fr"
  )$path("records_w3/streams/streams_short"),
  partitioning = arrow::schema(REGION = arrow::utf8())
)

streams_user_artist <- data_cloud %>%
  select(
    hashed_id,
    is_listened,
    listening_time,
    media_type,
    ts_listen,
    song_id = media_id
  ) %>%
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(
    media_type == "song",
    is_listened == 1,
    song_id > 0,
    year == 2022,
  ) %>%
  left_join(song_artist_weights, by = "song_id") %>%
  mutate(
    lt = if_else(listening_time < 0, 0, listening_time),
    w_l_play = lt * w_feat,
    w_n_play = w_feat
  ) %>%
  group_by(hashed_id, dz_artist_feat_id) %>%
  summarize(
    n_play = sum(w_n_play),
    l_play = sum(w_l_play),
    .groups = "drop"
  ) %>%
  collect()


# ------------------------------------

user_artist_shares <- streams_user_artist %>%
  group_by(hashed_id) %>%
  mutate(
    f_n_play = n_play / sum(n_play),
    f_l_play = l_play / sum(l_play)
  ) %>%
  ungroup()


# ------------------------------------

users <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")


t <- user_artist_shares %>%
    left_join(users, by = "hashed_id") %>%
    group_by(dz_artist_feat_id) %>%
    summarize(
      popularity_n = mean(f_n_play, na.rm = TRUE),
      popularity_l = mean(f_l_play, na.rm = TRUE),
      .groups = "drop"
    )


pop_control <- user_artist_shares %>% 
  left_join(users, by = "hashed_id") %>%
  filter(is_in_control_group) %>% 
  group_by(dz_artist_feat_id) %>%
  summarise(l_play = sum(l_play, na.rm=TRUE),
            n_play = sum(n_play, na.rm=TRUE),
            n_users = n_distinct(hashed_id)) %>% 
  mutate(f_l_play = l_play / sum(l_play, na.rm=TRUE),
         f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
  rename_with(~paste0("control_", .x), -dz_artist_feat_id)

pop_resp <- user_artist_shares %>% 
  left_join(users, by = "hashed_id") %>%
  filter(is_respondent) %>% 
  group_by(dz_artist_feat_id) %>%
  summarise(l_play = sum(l_play, na.rm=TRUE),
            n_play = sum(n_play, na.rm=TRUE),
            n_users = n_distinct(hashed_id)) %>% 
  mutate(f_l_play = l_play / sum(l_play, na.rm=TRUE),
         f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
  rename_with(~paste0("resp_", .x), -dz_artist_feat_id)



## workflow works, but stream share seems to be pretty different 
## (2% for jul instead of 1.17?) and there are 60% missings because
## ids don't match: why?

t <- pop_control %>% 
  select(dz_artist_feat_id, control_f_l_play, control_f_n_play)


test <- all_final %>% 
  left_join(t, by = c(dz_artist_id = "dz_artist_feat_id"))

test <- test %>% 
  mutate(control_f_l_play = control_f_l_play * 100,
         control_f_n_play = control_f_n_play * 100) 




