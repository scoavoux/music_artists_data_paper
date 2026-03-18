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



library(dplyr)

streams_combined <- full_join(
  streams_long,
  streams_short,
  by = "year",
  suffix = c("_long", "_short")
) %>%
  mutate(
    n_users = coalesce(n_users_long, 0) + coalesce(n_users_short, 0)
  ) %>%
  select(year, n_users) %>%
  arrange(year)