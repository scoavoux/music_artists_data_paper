dedup <- function(all, id, contacts = NULL, score, threshold){
  
  id <- rlang::sym(id)
  score <- rlang::sym(score)
  
  
  if(!is.null(contacts)) {
    coll_count <- coll_count <- contacts %>% 
      mutate(collection_count = as.integer(collection_count)) %>% 
      select(contact_id, collection_count)
    
    all <- all %>% 
      left_join(coll_count, by = "contact_id")
  }
  
  all_dup <- all %>% 
    add_count(!!id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(!!id) %>%
    mutate(max_score = max(!!score, na.rm = TRUE), # create score_share
           score_share = !!score / sum(!!score)) %>%
    arrange(desc(max_score), desc(!!score))
  
  to_keep <- all_dup %>% 
    filter(score_share > threshold) %>% 
    select(-c(score_share, collection_count, max_score, n))
  
  all <- all %>% 
    anti_join(to_keep, by = rlang::as_string(id)) %>% 
    bind_rows(to_keep)
  
  return(all)
}

dedup_all_ids <- function(all, contacts, threshold = 0.9) {
  
  list(
    deezer = dedup(
      all = all,
      id = "deezer_id",
      contacts = contacts,
      score = "collection_count",
      threshold = threshold
    ),
    contact = dedup(
      all = all_enriched,
      id = "contact_id",
      contacts = contacts,
      score = "pop",
      threshold = threshold
    ),
    musicbrainz = dedup(
      all = all_enriched,
      id = "musicbrainz_id",
      contacts = contacts,
      score = "pop",
      threshold = threshold
    )
  )
}



