# enrich the consolidated artists file "all" 
# with ids through unique name matches

patch_names <- function(all,
                        ref,
                        ref_id,
                        ref_name,
                        all_name,
                        all_id) {

  ref_id   <- rlang::ensym(ref_id)
  ref_name <- rlang::ensym(ref_name)
  all_name <- rlang::ensym(all_name)
  all_id   <- rlang::ensym(all_id)

  ## prepare reference table
  ref_clean <- ref %>%
    as_tibble() %>%
    mutate(across(where(is.integer), as.character)) %>%
    select(!!ref_id, !!ref_name) %>%
    filter(!is.na(!!ref_name)) %>%
    anti_join(all, by = setNames(rlang::as_string(ref_id),
                                 rlang::as_string(all_id)))

  ## rows in all missing IDs
  miss <- all %>%
    filter(is.na(!!all_id))

  ## unique name-based matches
  matches <- miss %>%
    inner_join(ref_clean,
               by = setNames(rlang::as_string(ref_name),
                             rlang::as_string(all_name))) %>%
    group_by(!!all_name) %>%
    mutate(n = n()) %>%
    filter(n == 1) %>%
    ungroup()
  
  # subset wanted cols
  id_y <- paste0(rlang::as_string(ref_id), ".y")
  
  matches <- matches %>%
    select(
      !!all_name,
      !!rlang::as_string(ref_id) := !!rlang::sym(id_y),
      deezer_id
    )    

  return(matches)
}



mbz_from_wiki <- function(all, wiki, mbz_patch, wiki_mbz_patch){
  
  mbz_missing <- all %>% 
    filter(is.na(musicbrainz_id)) 
  
  # inner_join of the missing mbz cases with wikidata's mbz
  mbz_from_wiki <- mbz_missing %>% 
    inner_join(wiki, by = "deezer_id") %>% 
    mutate(musicbrainz_id = musicbrainz_id.y) %>% 
    filter(!is.na(musicbrainz_id)) %>% 
    
    #filter(name != wiki_name) %>% # name matches are handled by patch_names already!
    distinct(deezer_id, musicbrainz_id, .keep_all = T) %>% 
    select(deezer_id, name, wiki_name, musicbrainz_id)
  
  return(mbz_from_wiki)
}


 

# patch_contact_names <- function(contacts, all){
#   
#   ## prepare contacts
#   contacts_ref <- contacts %>%
#     as_tibble %>%
#     mutate_if(is.integer, as.character) %>%   # contact_id to str
#     select(contact_id, contact_name) %>% # id cols only
#     filter(!is.na(contact_name)) %>% # contacts with names only
#     anti_join(all, by = "contact_id") # contacts not already in all only
#   
#   
#   # prepare all
#   miss <- all %>% 
#     filter(is.na(contact_id))
#   
#   # make ALL unique matches --- filter by missing in all later
#   contact_names_patch <- miss %>% # changed miss to all!
#     inner_join(contacts, by = c(name = "contact_name")) %>%
#     group_by(name) %>%
#     mutate(n = n()) %>%
#     arrange(desc(n)) %>%
#     filter(n == 1) %>%  # keep unique matches only
#     select(name, 
#            contact_id = "contact_id.y",
#            deezer_id)
#   # distinct(deezer_id, .keep_all = T) # leave out for now
#   
#   return(contact_names_patch)
# }























