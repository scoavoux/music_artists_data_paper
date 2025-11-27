#### create streams df with cols 1. song_id and 2. f_n_plays
load_streams <- function() {
  
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
    filter(media_type == "song", is_listened == 1, year == 2022) %>% 
    group_by(song_id) %>% 
    summarize(n_play = n()) %>% 
    mutate(f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
    select(song_id, f_n_play)
  
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
    filter(is_listened == 1, year != 2022) %>% 
    group_by(song_id) %>% 
    summarize(n_play = n()) %>% 
    mutate(f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
    select(song_id, f_n_play)
  
  long_streams <- collect(query_short)
  short_streams <- collect(query_long)
  
  streams <- full_join(short_streams, long_streams)
  
  return(streams)
}
















