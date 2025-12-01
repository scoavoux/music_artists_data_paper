## bind items and remove to_remove_file.
## temporary: split up in old and new
bind_items <- function(items_old, items_new, to_remove = to_remove_file){
  require(tidyverse)
  require(tidytable)
  
  s3 <- initialize_s3()

  items_old <- s3$get_object(Bucket = "scoavoux", 
                             Key = items_old)$Body %>% 
    read_parquet(col_select = c("song_id", 
                                "artist_id", 
                                "artists_ids"))
  
  items_new <- s3$get_object(Bucket = "scoavoux", 
                             Key = items_new)$Body %>% 
    read_parquet(col_select = c("song_id", 
                                "artist_id", 
                                "artists_ids"))
  
  items <- bind_rows(items_old, items_new) %>% 
    distinct()
  
  # remove artists in to_remove_file
  items <- items %>% 
    anti_join(to_remove) %>% 
    select(song_id,
           deezer_feat_ids = "artists_ids",
           deezer_id = "artist_id") %>% 
    filter(!is.na(deezer_id)) %>% 
  
  #loginfo("bound items and new items together. removed artists in to_remove_file.csv.")
  
  return(items)
}

