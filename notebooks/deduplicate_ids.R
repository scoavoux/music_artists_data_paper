

## reprex
dat <- tibble(deezer_id = c(1,1,2,2,3,4),
              contact_id = c(33,44,55,66,77,88),
              name = c("beatles", "beatles", "stones",
                       "stones", "nekfeu", "lomepal"))



## function to rule over duplicates
## takes dataframe as input, returns deduplicated dataframe
dedup <- function(data, ref_id, dup_id, name){
  
  require(dplyr)
  
  ref_id <- rlang::ensym(ref_id)
  dup_id <- rlang::ensym(dup_id)
  name <- rlang::ensym(name)
  
  id_map <- data %>%
    distinct(!!ref_id, !!name, .keep_all = T) %>%
    group_by(!!dup_id, !!name) %>%  # or some grouping logic you define
    mutate(new_id = first(!!dup_id)) %>%
    ungroup() %>% 
    select(!!dup_id, new_id)
  
  dat_clean <- data %>%
    inner_join(id_map, by = rlang::as_string(dup_id)) %>%
    mutate(!!dup_id := new_id) %>%
    select(-new_id)
  
  return(dat_clean)

}

dat

dedup(data = d,
      dup_id = "contact_id",
      ref_id = "deezer_id",
      name = "name")

































