# binds all "raw" id files to one dataset

consolidate_artists <- function(artists, 
                                mbz_deezer, 
                                contacts, 
                                manual_search,
                                wiki) {
  
  library(dplyr)
  library(stringr)
  library(logging)
  

  # PROCESS RAW -------------------------------------------
  
  # musicbrainz keys
  mbz_deezer <- mbz_deezer %>% 
    filter(!is.na(deezer_id)) %>%
    distinct(deezer_id, musicbrainz_id, mbz_name) # need to distinct bc dropping spotify etc leaves ~22k duplicates
  
  # join mbz and contacts beforehand
  mbz_deezer_contacts <- mbz_deezer %>% 
    left_join(contacts, by = "musicbrainz_id")
  
  # wiki names
  wiki <- wiki %>% 
    filter(!is.na(deezer_id)) %>%
    select(deezer_id, wiki_name)
  
  # JOIN ALL ---------------------------------------------
  all <- artists %>% 
    left_join(mbz_deezer_contacts, by = "deezer_id") %>% 
    # left_join(contacts, by = "musicbrainz_id") %>% 
    left_join(manual_search, by = "deezer_id") %>% 
    left_join(wiki, by = "deezer_id") %>% # added wiki for names
    mutate(contact_id = coalesce(contact_id.x, contact_id.y),
           collection_count = as.integer(collection_count),
           collection_count = ifelse(is.na(collection_count), 0, collection_count)
    ) %>% 
    select(name, contact_name, mbz_name, wiki_name, deezer_id, 
           musicbrainz_id, contact_id, pop, collection_count) %>% 
    distinct(deezer_id, contact_id, musicbrainz_id, .keep_all = TRUE) %>%  # !!!
    as_tibble()

  #loginfo("stream share after first consolidation:")
  #cleanpop(all)
  
  return(all)
}
















