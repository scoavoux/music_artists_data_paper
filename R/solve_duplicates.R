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


# remaining duplicates after these steps are
# 1. deezer dups with equivalent collection_counts
# 2. 

solve_remaining_dups <- function(){}
  
## if both collection_count > ...
## merge artists, keep co and mbz with higher collection count



tar_load(all_before_dedup)

test <- all_before_dedup %>%
  dedup(
    id = "deezer_id",
    score = "collection_count"
  )



wailers <- all_before_dedup %>% 
  filter(str_detect(name, "Wailers"))
wailers


dedup_col <- function(all){
  
  all_dup <- all %>% 
    add_count(deezer_id) %>% 
    filter(n > 1) %>% # duplicates
    group_by(deezer_id) %>%
    mutate(col_share = collection_count / sum(collection_count)) # create score_share 
  
  to_keep <- all_dup %>% 
    filter(col_share == max(col_share)) %>% ## formerly score_share > threshold
    select(-c(collection_count, n))
  
  all <- all %>% 
    mutate(col_share = NA_real_) %>%
    anti_join(to_keep, by = "deezer_id") %>% 
    bind_rows(to_keep)
  
  return(all)
}



wailers <- all_before_dedup %>% 
  filter(str_detect(name, "Wailers"))
wailers

test <- dedup_col(all = wailers)
test



all_dup <- wailers %>% 
  add_count(deezer_id) %>% 
  filter(n > 1) %>% # duplicates
  group_by(deezer_id) %>%
  mutate(col_share = collection_count / sum(collection_count)) # create score_share 

to_keep <- all_dup %>% 
  filter(col_share == max(col_share)) %>% ## formerly score_share > threshold
  select(-c(collection_count, n))

all <- wailers %>% 
  anti_join(to_keep, by = "deezer_id") %>% 
  bind_rows(to_keep)



## DELETE COLLECTION_COUNT == 0 before 















