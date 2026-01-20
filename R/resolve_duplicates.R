# among the duplicate deezer names for which there is one single contact_name 
# and contact_id, find the cases where one duplicate is much more popular than the others
# try different thresholds

patch_deezer_dups <- function(ref, 
                              ref_id, 
                              ref_name,
                              all, 
                              all_id = "deezer_id", 
                              all_name = "name"){
  
  require(dplyr)
  require(logging)
  
  loginfo("allgood")
  
  ref_id   <- rlang::sym(ref_id)
  ref_name <- rlang::sym(ref_name)
  all_name <- rlang::sym(all_name)
  all_id   <- rlang::sym(all_id)
  
  loginfo("allgood")
  
  # for each name in all, compute fraction of streams held by one homonym
  # and filter the clear cases missing contact_ids
  all_pop_share <- all %>% 
    group_by(name) %>% # maybe: name, deezer_id?
    mutate(pop_share = pop / sum(pop)) %>% 
    filter(pop_share > 0.90) %>% 
    filter(is.na(!!ref_id))
  
  loginfo("allgood")
  
  ## subset unique ref names
  unique_ref <- ref %>% 
    add_count(!!ref_name) %>% 
    filter(n == 1) %>% # unique names only
    select(!!ref_id, !!ref_name)
  
  loginfo("allgood")
  
  matches <- patch_names(all = all_pop_share,
              ref = unique_ref,
              ref_id = ref_id,
              ref_name = ref_name,
              all_name = all_name,
              all_id = all_id)
  
  return(matches)

}


patch_contact_dups <- function(all, ref, ref_id, ref_names){
  
  ref_id   <- rlang::ensym(ref_id)
  ref_name <- rlang::ensym(ref_name)
  all_name <- rlang::ensym(all_name)
  all_id   <- rlang::ensym(all_id)
  
  # ------- join to missing contact_data in all
  
  # keep this condition for now: adding the other variables adds like 30 cases
  # but unsure about the cases (e.g., there are weird ones with very few albums)
  ref_unique <- ref %>% 
    filter(col_share > 0.9) %>% 
    select(!!ref_name, !!ref_id)
  
  # important: subset all to unique names missing in contacts
  all_unique_ref <- all %>% 
    add_count(name) %>% 
    filter(n == 1) %>% 
    filter(is.na(!!ref_id))
  
  ## this is handled by patch_names...
  added_contacts <- patch_names(
    miss = all_unique_co,
    ref = co_unique,
    all_name = "name",
    ref_name = "contact_name",
    ref_id = "contact_id"
  )

  return(contacts_dup_patch)
}



















