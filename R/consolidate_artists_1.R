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
    filter(!is.na(deezerID)) %>%
    mutate_if(is.integer, as.character) %>% 
    distinct(deezerID, musicBrainzID, mbz_name) # need to distinct bc dropping spotify etc leaves ~22k duplicates
  
  # contacts keys
  contacts <- contacts %>% 
    mutate_if(is.integer, as.character) %>% 
    select(contact_id, contact_name, mbz_id, spotify_id)
  
  # sam's manual searches
  manual_search <- manual_search %>% 
    mutate_if(is.integer, as.character) 
  
  # JOIN ALL ---------------------------------------------
  all <- artists %>% 
    left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
    left_join(contacts,  by = c(musicBrainzID = "mbz_id")) %>% 
    left_join(manual_search, by = "deezer_id") %>% 
    mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
    select(name, contact_name, mbz_name, deezer_id, 
           musicBrainzID, contact_id, pop) %>% 
    as_tibble()

  cleanpop_1 <- cleanpop(all)
  loginfo(str_glue("stream share after first consolidation: {cleanpop_1}"))

  return(all)
}




























