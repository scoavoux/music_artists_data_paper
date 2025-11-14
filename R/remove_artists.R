remove_artists <- function(items = items,
                           to_remove = "data/artists_to_remove.csv"){
  
  to_remove <- fread(to_remove)
  
  # artist-song mapping to take care of duplicated artists 
  # To remove "fake" artists: accounts that compile anonymous music
  to_remove <- to_remove %>% 
    select(artist_id)
  
    clean_items <- items %>% 
    anti_join(to_remove) %>% 
    #left_join(senscritique_mb_deezer_id) %>%  # change this when new artist_id is set
    #mutate(artist_id = if_else(!is.na(consolidated_artist_id), 
                               #consolidated_artist_id, 
                               #artist_id)) %>% 
    select(song_id, artist_id) %>% 
    filter(!is.na(artist_id))
  
  return(clean_items)
}

