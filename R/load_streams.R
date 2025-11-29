#### rbind short and long streams 
#### create streams df with cols song_id and f_n_plays
#### remove songs with no listens, negative song_ids

load_streams <- function() {
  
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
    group_by(song_id) %>% 
    summarize(n_play = n()) %>% 
    select(song_id, n_play)
  
  # load and filter streams_long
  data_cloud <- arrow::open_dataset(
    source =   arrow::s3_bucket("scoavoux",
                                endpoint_override = "minio.lab.sspcloud.fr"
    )$path("records_w3/streams/streams_long"),
    partitioning = arrow::schema(REGION = arrow::utf8())
  )
  
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
    summarize(n_play = n()) %>% 
    select(song_id, n_play)
  
  long_streams <- collect(query_short)
  short_streams <- collect(query_long)
  
  # bind rows and compute popularity (fraction of all plays)
  streams <- bind_rows(short_streams, long_streams) %>%
    group_by(song_id) %>%
    summarize(n_play = sum(n_play)) %>%
    mutate(f_n_play = n_play / sum(n_play))
  
  loginfo(cat("streams loaded, with", nrow(streams), "rows."))
  
  return(streams)
}

# write it to interim
# write_parquet(streams, "data/interim/clean_streams.parquet", compression = "snappy")
# loginfo("clean_streams.parquet written".)









