join_items <- function(to_remove_file){
  require(tidyverse)
  require(tidytable)
  
  s3 <- initialize_s3()
  
  # items to take care of duplicated artists 
  # To remove "fake" artists: accounts that compile anonymous music
  to_remove <- to_remove_file %>% 
    select(artist_id)
  
  items_old <- s3$get_object(Bucket = "scoavoux", 
                             Key = "records_w3/items/songs.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))
  items_new <- s3$get_object(Bucket = "scoavoux", 
                             Key = "records_w3/items/song.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))  
  items <- bind_rows(items_old, items_new) %>% 
    distinct()
  
  items <- items %>% 
    anti_join(to_remove) %>% 
    #left_join(senscritique_mb_deezer_id) %>% 
    #mutate(artist_id = if_else(!is.na(consolidated_artist_id), consolidated_artist_id, artist_id)) %>% 
    select(song_id, artist_id) %>% 
    filter(!is.na(artist_id))
  
  return(items)
}

