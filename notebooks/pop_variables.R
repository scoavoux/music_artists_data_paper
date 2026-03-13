tar_load(streams)
tar_load(items)


library(dplyr)

# load and filter streams_short
data_cloud <- arrow::open_dataset(
  source =   arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr"
  )$path("records_w3/streams/streams_short"),
  partitioning = arrow::schema(REGION = arrow::utf8())
)

query_short <- data_cloud %>% 
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
  left_join(items, by = "song_id") %>% 
  group_by(hashed_id, deezer_feat_id) %>% 
  summarize(l_play = sum(lt), 
            n_play = n()) %>% 
  ungroup()

query_long <- data_cloud %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         song_id) %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(is_listened == 1, 
         year != 2022,
         song_id > 0) %>% # negative song_ids
  group_by(song_id) %>% 
  summarize(n_play = n(),
            listening_time = sum(listening_time)) %>% 
  select(song_id, n_play, listening_time)


short_streams <- collect(query_short)



short_streams <- collect(query_short)
long_streams <- collect(query_long)

# bind rows and compute popularity (fraction of all plays)
streams <- bind_rows(short_streams, long_streams) %>%
  group_by(hashed_id, artist_id) %>%
  summarize(n_play = sum(n_play),
            l_play = sum(listening_time))






t <- short_streams %>% 
  slice(1:100000)

t <- t %>% 
  as_tibble()


t %>% 
  distinct(hashed_id)
i





