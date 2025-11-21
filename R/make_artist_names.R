make_artist_names <- function(to_remove_file) {
  
    s3 <- initialize_s3()
    
    names <- s3$get_object(Bucket = "scoavoux", 
                         Key = "records_w3/items/artists_data.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("artist_id", "name"))

    
    to_remove <- to_remove_file %>% 
      select(artist_id)
      
    names <- names %>% 
      anti_join(to_remove)

    return(names)
}

