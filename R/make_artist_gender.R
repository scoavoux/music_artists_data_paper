make_artist_gender <- function(){

  s3 <- initialize_s3()
  
  gender <- s3$get_object(Bucket = "scoavoux", 
                          Key = "musicbrainz/mbz_gender.csv")$Body %>% 
    read_csv(show_col_types = F) %>% 
    rename(mbid = "gid")
  
  mbid <- s3$get_object(Bucket = "scoavoux", 
                        Key = "musicbrainz/mbid_deezerid.csv")$Body %>% 
    read_csv(show_col_types = F)
  
  artist_gender <- inner_join(mbid, gender) %>% 
    select(artist_id, gender) %>% 
    slice(1, .by = artist_id)
  
  return(artist_gender)
}
