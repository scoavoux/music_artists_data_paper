# among the duplicate deezer names for which there is one single contact_name 
# and contact_id, find the cases where one duplicate is much more popular than the others
# try different thresholds

patch_deezer_dups <- function(ref, 
                              ref_id, 
                              ref_name,
                              all, 
                              all_name = "name"){
  
  require(dplyr)
  require(logging)
  
  ref_id   <- rlang::sym(ref_id)
  ref_name <- rlang::sym(ref_name)
  all_name <- rlang::sym(all_name)

  # for each name in all, compute fraction of streams held by one homonym
  # and filter the clear cases missing contact_ids
  all_pop_share <- all %>% 
    group_by(name) %>% # maybe: name, deezer_id?
    mutate(pop_share = pop / sum(pop)) %>% 
    filter(pop_share > 0.90) %>% 
    filter(is.na(!!ref_id))
  
  ## subset unique ref names
  unique_ref <- ref %>% 
    add_count(!!ref_name) %>% 
    filter(n == 1) %>% # unique names only
    select(!!ref_id, !!ref_name)
  
  matches <- patch_names(all = all_pop_share,
              ref = unique_ref,
              ref_id = ref_id,
              ref_name = ref_name,
              all_name = all_name)
  
  return(matches)

}

# --------------------------------------------------------------

# resolve contact name duplicates by share of collection_count they have
# then patch them to unique* deezer names
patch_contact_dups <- function(all, contacts){
  
  # ----------- subset all to unique names missing contact_ids
  ## *added 0.9 filtering condition to include some deezer dups!
  all_unique_co <- all %>%
    group_by(name) %>% # maybe: name, deezer_id?
    mutate(pop_share = pop / sum(pop)) %>% 
    filter(pop_share > 0.90) %>% 
    add_count(name) %>%
    filter(n == 1) %>%
    filter(is.na(contact_id)) %>% 
    select(name, contact_name, deezer_id, contact_id)
  
  # ------------ prepare contacts
  co_unique <- contacts %>% 
    # keep this condition for now: adding the other variables adds like 30 cases
    # but unsure about the cases (e.g., there are weird ones with very few albums)
    filter(collection_count > 0) %>% # remove irrelevant artists 
    group_by(contact_name) %>% 
    mutate(col_share = collection_count / sum(collection_count),
           prod_share = product_count / sum(product_count),
           gen_like_share = gen_like_count / sum(gen_like_count)) %>% 
    filter(col_share > 0.9) %>% 
    select(contact_name, contact_id)
  
  
  # ------- patch to missing contact_data in all
  matches <- patch_names(all = all_unique_co,
                   ref = co_unique,
                   ref_id = "contact_id",
                   ref_name = "contact_name",
                   all_name = "name")
  
  return(matches)
}





