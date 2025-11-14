### map streams_short to clean_items$artist_id
make_user_data <- function(clean_items){
  require(aws.s3)
  require(lubridate)
  
  data_cloud <- arrow::open_dataset(
    source = arrow::s3_bucket(
      "scoavoux",
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
    
    # filter by listened songs in 2022
    filter(media_type == "song", is_listened == 1, year == 2022) %>% 
    inner_join(clean_items) %>% 
    group_by(hashed_id, artist_id) %>% 
    summarize(l_play = sum(lt), 
              n_play = n()) %>% 
    ungroup()
  
  user_data <- collect(query)
  
  return(user_data)
}













