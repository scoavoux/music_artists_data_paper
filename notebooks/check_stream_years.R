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
         song_id > 0) %>%
  group_by(year) %>% 
  summarise(n_users = n_distinct(hashed_id)) %>% 
  collect()


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
         song_id > 0) %>% 
  group_by(year) %>% 
  summarise(n_users = n_distinct(hashed_id)) %>% 
  collect()


streams_short
streams_long <- streams_long %>% 
  arrange(year)


streams_combined <- full_join(
  streams_long,
  streams_short,
  by = "year",
  suffix = c("_long", "_short")
) %>%
  mutate(
    n_users = coalesce(n_users_long, 0) + coalesce(n_users_short, 0)
  ) %>%
  arrange(year)

write.csv2(streams_combined, "data/dz_users_per_year.csv")






