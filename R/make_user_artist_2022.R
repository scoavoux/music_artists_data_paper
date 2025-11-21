make_user_artist_2022 <- function(items
                                  #, to_remove_file
                                  ){
  require(aws.s3)
  require(lubridate)
  
  # items to take care of duplicated artists 
  # To remove "fake" artists: accounts that compile anonymous music
  #to_remove <- to_remove_file %>% 
    #select(artist_id)
  
    items <- items %>% 
    #anti_join(to_remove) %>% 
    #left_join(senscritique_mb_deezer_id) %>% 
    #mutate(artist_id = if_else(!is.na(consolidated_artist_id), consolidated_artist_id, artist_id)) %>% 
   select(song_id, artist_id) %>% 
   filter(!is.na(artist_id))
  
  data_cloud <- arrow::open_dataset(
    source =   arrow::s3_bucket(
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
    filter(media_type == "song", is_listened == 1, year == 2022) %>% 
    inner_join(items) %>% 
    group_by(hashed_id, artist_id) %>% 
    summarize(l_play = sum(lt), 
              n_play = n()) %>% 
    ungroup()
  short_streams <- collect(query)
  return(short_streams)
}













