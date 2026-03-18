tar_load(dz_songs)
options(arrow.skip_nul = TRUE)

library(tidyr)

# -------------------
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
path_long <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/streams/streams_long")
)

streams_long <- path_long %>%
  select(hashed_id,
         is_listened,
         ts_listen,
         listening_time,
         song_id
         ) %>%
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>%
  filter(is_listened == 1,
         year != 2022,
         song_id > 0) %>%
  inner_join(song_artist_weights, by = "song_id") %>% 
  inner_join(users, by = c("hashed_id")) %>%
  select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)


path_short <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                            endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/streams/streams_short")
)

streams_short <- path_short %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         media_type, 
         song_id = "media_id"
  ) %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(media_type == "song", 
    is_listened == 1, 
    year == 2022,
    song_id > 0) %>% 
  inner_join(song_artist_weights, by = "song_id") %>% 
  inner_join(users, by = c("hashed_id")) %>%
  select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)


streams

streams <- bind_rows(streams_short, streams_long)


# make n_play at group-song level
plays <- streams %>%
  group_by(song_id, dz_artist_feat_id, 
           is_respondent, w_feat) %>%
  summarise(n_play = n(),
            .groups = "drop") %>% 
  mutate(n_play = n_play * w_feat) %>% 
  select(song_id, dz_artist_feat_id, is_respondent, n_play)


# make n_users at group-artist levels
artist_user_counts <- streams_short %>%
  group_by(dz_artist_feat_id, is_respondent) %>%
  summarise(
    dz_stream_user_count = n_distinct(hashed_id),
    .groups = "drop"
  ) %>% 
  select(dz_artist_feat_id, is_respondent, dz_stream_user_count)


# ---------------------------------------------------------------
# ARTIST POPULARITY
artist_popularity <- plays %>%
  
  # aggregate dz_stream_count to group
  group_by(dz_artist_feat_id, is_respondent) %>%
  summarise(
    dz_stream_count = sum(n_play, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  
  # add dz_stream_user_count (agg by group already)
  inner_join(
    artist_user_counts,
    by = c("dz_artist_feat_id", "is_respondent")
  ) %>%
  group_by(is_respondent) %>%
  mutate(
    dz_stream_share = dz_stream_count / sum(dz_stream_count) * 100
  ) %>%
  ungroup() %>% 
  
  # reshape to wide
  mutate(group = ifelse(is_respondent == 1, "respondent", "control")) %>%
  select(-is_respondent) %>%
  pivot_wider(
    names_from = group,
    values_from = c(dz_stream_count, dz_stream_user_count, dz_stream_share),
    names_glue = "{.value}_{group}"
  ) %>% 
  
  collect()


head(artist_popularity)


# reshape to wide
t <- artist_popularity %>% 
  mutate(group = ifelse(is_respondent == 1, "respondent", "control")) %>%
  select(-is_respondent) %>%
  pivot_wider(
    names_from = group,
    values_from = c(dz_stream_count, dz_stream_user_count, dz_stream_share),
    names_glue = "{.value}_{group}"
  )


tar_load(all_final)

test <- all_final %>% 
  left_join(t, by = c(dz_artist_id = "dz_artist_feat_id")) %>% 
  arrange(desc(dz_stream_count_control))


prop_na(t)




























