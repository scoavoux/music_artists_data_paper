# helper functions for the artist id issues 

# quick benchmark of stream shares
# input x is a dataframe with cols deezer_id, musicBrainzID, contact_id
cleanpop <- function(x){ 
  
  mbz <- x %>% 
    filter(!is.na(musicBrainzID)) %>% 
    distinct(deezer_id, .keep_all = T)

  contacts <- x %>%
    filter(!is.na(contact_id)) %>% 
    distinct(deezer_id, .keep_all = T)
  
  both <- x %>%
    filter(!is.na(musicBrainzID)) %>% 
    filter(!is.na(contact_id)) %>% 
    distinct(deezer_id, .keep_all = T)
  
  mbz_clean <- sum(mbz$pop) * 100
  contacts_clean <- sum(contacts$pop) * 100
  both_clean <- sum(both$pop) * 100
  
  cat("clean mbz ids:",mbz_clean,"%. \n")
  cat("clean contact ids:",contacts_clean,"%. \n")
  cat("complete cases:",both_clean,"%.")
  
}


# prop of nas inside a dataset
prop_na <- function(x) {
  
  nas <- function(x){
    sum(is.na(x)) / length(x)
  }
  
  lapply(x, nas)
  
}


# left join all with added pairings and coalesce id columns
left_join_coalesce <- function(x, y, by, col) {
  x %>% 
    left_join(y, by = by, suffix = c(".x", ".y")) %>% 
    mutate("{col}" := coalesce(.data[[paste0(col, ".x")]],
                               .data[[paste0(col, ".y")]])) %>% 
    select(-all_of(c(paste0(col, ".x"), paste0(col, ".y"))))
}

# extract unique name matches between the missing cases of all
# and enrich all with them
unique_name_match <- function(miss, ref, miss_name, ref_name, id_col) {
  miss %>% 
    inner_join(
      ref,
      by = setNames(ref_name, miss_name)
    ) %>% 
    rename(!!id_col := paste0(id_col, ".y")) %>% 
    group_by(.data[[miss_name]]) %>% 
    filter(n() == 1) %>% 
    ungroup() %>% 
    distinct(deezer_id, .keep_all = TRUE) %>% 
    select(deezer_id, all_of(id_col))
}



