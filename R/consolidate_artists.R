# binds all "raw" id files to one dataset

join_artist_ids <- function(artists, 
                                mbz_deezer, 
                                senscritique, 
                                manual_search,
                                wiki) {
  
  library(dplyr)
  library(stringr)
  library(logging)
  

  # PROCESS RAW -------------------------------------------
  
  # musicbrainz keys
  mbz_deezer <- mbz_deezer %>% 
    filter(!is.na(dz_artist_id)) %>%
    distinct(dz_artist_id, mbz_artist_id, mbz_name) # need to distinct bc dropping spotify etc leaves ~22k duplicates
  
  # join mbz and senscritique beforehand
  mbz_deezer_senscritique <- mbz_deezer %>% 
    left_join(senscritique, by = "mbz_artist_id")
  
  # wiki names
  wiki <- wiki %>% 
    filter(!is.na(dz_artist_id)) %>%
    select(dz_artist_id, wiki_name)
  
  # JOIN ALL ---------------------------------------------
  all <- artists %>% 
    left_join(mbz_deezer_senscritique, by = "dz_artist_id") %>% 
    # left_join(senscritique, by = "mbz_artist_id") %>% 
    left_join(manual_search, by = "dz_artist_id") %>% 
    left_join(wiki, by = "dz_artist_id") %>% # added wiki for names
    mutate(sc_artist_id = coalesce(sc_artist_id.x, sc_artist_id.y),
           collection_count = as.integer(collection_count),
           collection_count = ifelse(is.na(collection_count), 0, collection_count)
    ) %>% 
    select(dz_name, sc_name, mbz_name, wiki_name, 
           dz_artist_id, mbz_artist_id, sc_artist_id, 
           dz_stream_share, collection_count, n_ratings) %>% 
    distinct(dz_artist_id, sc_artist_id, mbz_artist_id, .keep_all = TRUE) %>%  # !!!
    as_tibble()

  #loginfo("stream share after first consolidation:")
  #cleandz_stream_share(all)
  
  return(all)
}
















