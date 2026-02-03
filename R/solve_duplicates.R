dedup <- function(all, id, score, threshold){
  
  require(logging)
  
  id <- rlang::sym(id)
  score <- rlang::sym(score)
  
  all_dup <- all %>% 
    add_count(!!id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(!!id) %>%
    mutate(score_share = !!score / sum(!!score)) # create score_share 
  
  to_keep <- all_dup %>% 
    filter(score_share > threshold) %>% 
    select(-c(score_share, collection_count, n))
  
  all <- all %>% 
    anti_join(to_keep, by = rlang::as_string(id)) %>% 
    bind_rows(to_keep)
  
  return(all)
}

dedup_all_ids <- function(all, threshold = 0.9) {
  
  all %>%
    dedup(
      id = "deezer_id",
      score = "collection_count",
      threshold = threshold
    ) %>%
    dedup(
      id = "contact_id",
      score = "pop",
      threshold = threshold
    ) %>%
    dedup(
      id = "musicbrainz_id",
      score = "pop",
      threshold = threshold
    )
}


# remaining duplicates after these steps are
# 1. deezer dups with equivalent collection_counts
# 2. 

solve_remaining_dups <- function(){}
  
## if both collection_count > ...
## merge artists, keep co and mbz with higher collection count












