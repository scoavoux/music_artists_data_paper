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
  
  cat("N:",nrow(x %>% distinct(deezer_id)),"\n")
  cat("clean mbz ids:",mbz_clean,"% // N:",nrow(mbz),"\n")
  cat("clean contact ids:",contacts_clean,"% // N:",nrow(contacts),"\n")
  cat("complete cases:",both_clean,"% // N:",nrow(both))
  
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

left_join_coalesce <- function(x, y, by, cols) {
  out <- left_join(x, y, by = by, suffix = c(".x", ".y"))
  
  for (col in cols) {
    out[[col]] <- coalesce(
      out[[paste0(col, ".x")]],
      out[[paste0(col, ".y")]]
    )
  }
  
  out %>% select(-ends_with(".x"), -ends_with(".y"))
}


# extract unique name matches between the missing cases of all
# and enrich all with them
unique_name_match <- function(miss, ref, miss_name, 
                              ref_name, id_col, 
                              out_name = ref_name) {
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
    transmute(
      deezer_id,
      !!id_col := .data[[id_col]],
      !!out_name := .data[[miss_name]]
    )
}



