# binds all "raw" id files to one dataset

consolidate_artists <- function(artists, 
                                mbz_deezer, 
                                contacts, 
                                manual_search) {
  
  library(dplyr)
  library(stringr)
  library(logging)
  

  # PROCESS RAW -------------------------------------------
  
  # musicbrainz keys
  mbz_deezer <- mbz_deezer %>% 
    filter(!is.na(deezer_id)) %>%
    mutate_if(is.integer, as.character) %>% 
    distinct(deezer_id, musicbrainz_id, mbz_name) # need to distinct bc dropping spotify etc leaves ~22k duplicates
  
  # contacts keys
  contacts <- contacts %>% 
    mutate_if(is.integer, as.character) %>%
    rename(musicbrainz_id = "mbz_id") %>% 
    select(contact_id, contact_name, musicbrainz_id, spotify_id)
  
  # sam's manual searches
  manual_search <- manual_search %>% 
    mutate_if(is.integer, as.character) %>% 
    rename(deezer_id = "artist_id")


  # JOIN ALL ---------------------------------------------
  all <- artists %>% 
    left_join(mbz_deezer, by = "deezer_id") %>% 
    left_join(contacts, by = "musicbrainz_id") %>% 
    left_join(manual_search, by = "deezer_id") %>% 
    mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
    select(name, contact_name, mbz_name, deezer_id, 
           musicbrainz_id, contact_id, pop) %>% 
    as_tibble()

  loginfo("stream share after first consolidation:")
  cleanpop(all)

  return(all)
}




























