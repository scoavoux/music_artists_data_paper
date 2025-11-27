library(targets)
library(tarchetypes)
library(dplyr)
library(data.table)

tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  repository = "aws", 
  repository_meta = "aws",
  error = "null",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "music_artists"
    )
  )
)

tar_source("R")

source("./R/load_data.R")

tar_load(items)

## ------------------------------------------------------------------------------------
## streams
data_cloud <- arrow::open_dataset(
  source =   arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr"
                              )$path("records_w3/streams/streams_short"),
  partitioning = arrow::schema(REGION = arrow::utf8())
)

query <- data_cloud %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         media_type, 
         song_id = "media_id") %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(media_type == "song", is_listened == 1, year == 2022) %>% 
  inner_join(items) %>% 
  group_by(song_id, artist_id) %>% 
  summarize(l_play = sum(lt), 
            n_play = n()) %>% 
  ungroup() %>% 
  mutate(f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
  select(song_id, f_n_play) %>% 
  distinct(song_id, .keep_all = TRUE)
  
short_streams <- collect(query)

data_cloud <- arrow::open_dataset(
  source =   arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr"
  )$path("records_w3/streams/streams_long"),
  partitioning = arrow::schema(REGION = arrow::utf8())
)

query <- data_cloud %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         song_id) %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(is_listened == 1, year != 2022) %>% 
  inner_join(items) %>% 
  distinct(song_id, .keep_all = TRUE) %>% 
  group_by(song_id, artist_id) %>% 
  summarize(l_play = sum(lt), 
            n_play = n()) %>% 
  ungroup() %>% 
  mutate(f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
  select(song_id, f_n_play)

long_streams <- collect(query)

streams <- full_join(short_streams, long_streams) # 8.88M

## ------------------------------------------------------------------------------------
## song to artist
tar_load(items)
items <- items %>% 
  distinct(song_id, .keep_all = TRUE)

streams_missing_in_items <- anti_join(streams, items) # no match --- need this?

streams_in_items <- inner_join(items, streams) # inner because drop if no match in items

nrow(streams_in_items) + nrow(streams_missing_in_items) # adds up

## ------------------------------------------------------------------------------------
## deezer names
artists_data <- load_s3("records_w3/items/artists_data.snappy.parquet")

artists_data <- artists_data %>% 
  select(artist_id, name) %>% 
  distinct(artist_id, .keep_all = TRUE)

items_artists <- inner_join(items, artists_data) 




df <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")
















