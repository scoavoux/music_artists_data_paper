# group dz_songs by artist and joins popularity
group_songs_by_artist <- function(dz_songs, dz_stream_data){
  
  dz_artists <- dz_songs %>% 
    ungroup() %>% 
    mutate(dz_artist_id = as.character(dz_artist_feat_id)) %>%  # ATTENTION: renaming feat_id to id here!!
    group_by(dz_artist_id) %>% 
    summarise(dz_name = first(dz_name),
              .groups = "drop") %>% 
    
    # NEW 18/03: ADD POP HERE!
    inner_join(dz_stream_data, by = "dz_artist_id") %>% 
    
    arrange(desc(n_plays))
  
  return(dz_artists)
}


# --------------- binds all "raw" id files to one dataset
join_artist_ids <- function(dz_songs, 
                            dz_stream_data,
                            mbz_deezer, 
                            senscritique, 
                            manual_search,
                            wiki) {
  
  library(dplyr)
  library(stringr)
  
  # group dz_songs by artist -----------------
  artists <- group_songs_by_artist(dz_songs, dz_stream_data)
  
  
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

  # JOIN ALL IDs TO ARTISTS ---------------------------------------------
  artists <- artists %>% 
    left_join(mbz_deezer_senscritique, by = "dz_artist_id") %>% 
    left_join(manual_search, by = "dz_artist_id") %>% 
    left_join(wiki, by = "dz_artist_id") %>% # add wiki for names
    mutate(sc_artist_id = coalesce(sc_artist_id.x, sc_artist_id.y),
           mbz_artist_id = coalesce(mbz_artist_id.x, mbz_artist_id.y),
           sc_collection_count = as.integer(sc_collection_count),
           sc_collection_count = ifelse(is.na(sc_collection_count), 0, sc_collection_count)
    ) %>% 
    select(dz_name, 
           sc_name, 
           mbz_name,
           wiki_name, 
           dz_artist_id, 
           mbz_artist_id, 
           sc_artist_id, 
           sc_collection_count, 
           starts_with("n_")) %>% # popularity metrics
    distinct(dz_artist_id, sc_artist_id, mbz_artist_id, .keep_all = TRUE) %>%  # !!!
    as_tibble()

  print_stream_share(artists)
  
  return(artists)
}









