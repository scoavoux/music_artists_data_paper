deduplicate_ids <- function(all){
  
  require(dplyr)
  
  all <- all %>% 
    
    group_by(dz_artist_id) %>% 
    mutate(colcount_share_by_dzdup = sc_collection_count / sum(sc_collection_count, na.rm = TRUE))  %>% 
    ungroup() %>% 
    
    group_by(sc_artist_id) %>%
    mutate(stream_share_by_scdup = n_plays / sum(n_plays, na.rm = TRUE)) %>%
    ungroup() %>%
    
    group_by(mbz_artist_id) %>%
    mutate(stream_share_by_mbzdup = n_plays / sum(n_plays, na.rm = TRUE)) %>%
    ungroup()
  
  # ------------------------------------------------------
  
  # only keep conflicts to speed up conflict identification
  dz_conflicts <- all %>% 
    add_count(dz_artist_id, name = "n_dz") %>% 
    mutate(keep_dz = is.na(dz_artist_id) | n_dz == 1) %>% 
    filter(keep_dz == FALSE)
  
  sc_conflicts <- all %>% 
    add_count(sc_artist_id, name = "n_sc") %>% 
    mutate(keep_sc = is.na(sc_artist_id) | n_sc == 1) %>% 
    filter(keep_sc == FALSE)
  
  mbz_conflicts <- all %>% 
    add_count(mbz_artist_id, name = "n_mbz") %>% 
    mutate(keep_mbz = is.na(mbz_artist_id) | n_mbz == 1) %>% 
    filter(keep_mbz == FALSE)
  
  # ---------------------------------------------------------
  
  dz_losers <- dz_conflicts %>% 
    group_by(dz_artist_id) %>% 
    mutate(keep_dz = colcount_share_by_dzdup == max(colcount_share_by_dzdup, na.rm = TRUE)) %>% 
    filter(keep_dz == FALSE)
  
  sc_losers <- sc_conflicts %>% 
    group_by(sc_artist_id) %>% 
    mutate(keep_sc = stream_share_by_scdup == max(stream_share_by_scdup, na.rm = TRUE)) %>% 
    filter(keep_sc == FALSE)
  
  mbz_losers <- mbz_conflicts %>% 
    group_by(mbz_artist_id) %>% 
    mutate(keep_mbz = stream_share_by_mbzdup == max(stream_share_by_mbzdup, na.rm = TRUE)) %>% 
    filter(keep_mbz == FALSE)
  
  # -------------------------------------------------------
  
  # remove from all
  
  all_dedup <- all %>% 
    anti_join(dz_losers, by = c("dz_artist_id", "sc_artist_id", "mbz_artist_id")) %>% 
    anti_join(sc_losers, by = c("dz_artist_id", "sc_artist_id", "mbz_artist_id")) %>% 
    anti_join(mbz_losers, by = c("dz_artist_id", "sc_artist_id", "mbz_artist_id")) %>% 
    
    # count remaining duplicates
    add_count(dz_artist_id, name = "n_deezer") %>% 
    add_count(sc_artist_id, name = "n_sc") %>% 
    add_count(mbz_artist_id, name = "n_mbz") %>% 
    
    # set NAs on n counts
    mutate(
      n_deezer = ifelse(n_deezer > 1000, NA, n_deezer),
      n_sc = ifelse(n_sc > 1000, NA, n_sc),
      n_mbz = ifelse(n_mbz > 1000, NA, n_mbz)
    ) %>% 
    
    # keep unique only
    filter(n_deezer == 1 | is.na(n_deezer)) %>% 
    filter(n_sc == 1 | is.na(n_sc)) %>% 
    filter(n_mbz == 1 | is.na(n_mbz)) %>% 
    
    select(-c(n_deezer, n_sc, n_mbz))
  
  
  print_stream_share(all_dedup)
  
  ## check filtered out cases
  ## save to csv somewhere
  removed_duplicates <- all %>% 
    anti_join(all_dedup, by = c("dz_artist_id", "sc_artist_id", "mbz_artist_id"))

  write_s3(removed_duplicates, "interim/removed_duplicates.csv")
  
  return(all_dedup)
}





