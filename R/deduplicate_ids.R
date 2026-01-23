



dat <- tibble(deezer_id = c(1,1,2,2,3,4),
              contact_id = c(33,44,55,66,77,88),
              name = c("beatles", "beatles", "stones", 
                       "stones", "nekfeu", "lomepal"))

dat


dat %>%
  distinct(deezer_id, name) #%>% # same deezer_id, same name!
  group_by(deezer_id, name) %>%  # or some grouping logic you define
  mutate(canonical_id = first(deezer_id)) %>%
  ungroup()



t <- co_dups_mbz_id %>%
  distinct(mbz_id, contact_name, .keep_all = T) %>% # same mbid, same name!
  group_by(contact_id, contact_name) %>%
  mutate(canonical_id = first(contact_id)) %>%
  ungroup()

co_dups_mbz_id
t



## function to rule over duplicates

## takes dataframe as input

## returns deduplicated dataframe

dedup <- function(data, ref_id, dup_id, name){
  
  ref_id <- rlang::ensym(ref_id)
  dup_id <- rlang::ensym(dup_id)
  name <- rlang::ensym(name)
  
  
  id_map <- data %>%
    distinct(!!ref_id, !!name, .keep_all = T) %>%
    group_by(!!dup_id, !!name) %>%  # or some grouping logic you define
    mutate(canonical_id = first(!!dup_id)) %>%
    ungroup()
  
  dat_clean <- data %>%
    left_join(id_map, by = rlang::as_string(dup_id)) %>%
    mutate(!!dup_id := canonical_id) %>%
    select(-canonical_id)

}

dedup(data = dat,
      dup_id = "contact_id",
      ref_id = "deezer_id",
      name = "name")

































