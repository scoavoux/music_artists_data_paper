dedup <- function(all, id, score){
  
  require(logging)
  
  id <- rlang::sym(id)
  score <- rlang::sym(score)
  
  all_dup <- all %>% 
    add_count(!!id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(!!id) %>%
    mutate(score_share = !!score / sum(!!score)) # create score_share 
  
  to_keep <- all_dup %>% 
    filter(score_share == max(score_share)) %>% ## formerly score_share > threshold
    select(-c(collection_count, n))
  
  all <- all %>% 
    anti_join(to_keep, by = rlang::as_string(id)) %>% 
    bind_rows(to_keep)
  
  return(all)
}

dedup_all_ids <- function(all) {
  
  all %>%
    dedup(
      id = "deezer_id",
      score = "collection_count"
    ) %>%
    dedup(
      id = "contact_id",
      score = "pop"
    ) %>%
    dedup(
      id = "musicbrainz_id",
      score = "pop"
    )
}


dedup_col <- function(all){
  
  # compute score share columns
  # could add mbz too, but it concerns almost no cases
  all <- all %>% 
    #filter(!is.na(contact_id)) %>%
    group_by(deezer_id) %>% 
    mutate(dz_col_share = collection_count / sum(collection_count)) %>% 
    ungroup() %>% 
    group_by(contact_id) %>% 
    mutate(co_pop_share = pop / sum(pop)) %>% 
    ungroup() 
  
  to_keep_dz <- all %>% 
    add_count(deezer_id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(deezer_id) %>%
    mutate(dz_col_share = collection_count / sum(collection_count)) %>% # create score_share 
    filter(dz_col_share == max(dz_col_share)) ## formerly score_share > threshold
    
  to_keep_co <- all %>% 
    add_count(contact_id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(deezer_id) %>%
    mutate(co_pop_share = collection_count / sum(collection_count)) %>%
    filter(co_pop_share == max(co_pop_share))
  
  # bind "winners"
  all <- all %>% 
    anti_join(to_keep_dz, by = "deezer_id") %>% 
    bind_rows(to_keep_dz) %>% 
    anti_join(to_keep_co, by = "deezer_id") %>% 
    bind_rows(to_keep_co)
  
  return(all)
}







