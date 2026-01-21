# enrich the consolidated artists file "all" 
# with ids found through unique name matches

patch_names <- function(all,
                        ref,
                        ref_id,
                        ref_name,
                        all_name) {

  require(logging)
  
  loginfo("up and running")
  
  ref_id   <- rlang::sym(ref_id)
  ref_name <- rlang::sym(ref_name)
  all_name <- rlang::sym(all_name)

  loginfo("up and running")
  
  ## prepare reference table
  ref_clean <- ref %>%
    as_tibble() %>%
    mutate(across(where(is.integer), as.character)) %>%
    select(!!ref_id, !!ref_name) %>%
    filter(!is.na(!!ref_name)) %>%
    anti_join(all, by = setNames(rlang::as_string(ref_id),
                                 rlang::as_string(ref_id)))

  loginfo("up and running")
  
  ## rows in all missing IDs
  miss <- all %>%
    filter(is.na(!!ref_id))

  loginfo("up and running")
  
  ## unique name-based matches
  matches <- miss %>%
    inner_join(ref_clean,
               by = setNames(rlang::as_string(ref_name),
                             rlang::as_string(all_name))) %>%
    group_by(!!all_name) %>%
    mutate(n = n()) %>%
    filter(n == 1) %>%
    ungroup()
  
  loginfo("up and running")
  
  
  # subset wanted cols
  id_y <- paste0(rlang::as_string(ref_id), ".y")
  
  loginfo("up and running")
  
  matches <- matches %>%
    select(
      !!all_name,
      !!rlang::as_string(ref_id) := !!rlang::sym(id_y),
      deezer_id
    ) 
    # add ref_name so it gets updated too
    # left_join it (instead of select) because wiki_name
    # does not exist in all
  
  loginfo("up and running")
  
  
  return(matches)
  
  }
  


  
  



mbz_from_wiki <- function(all, wiki){
  
  mbz_missing <- all %>% 
    filter(is.na(musicbrainz_id)) 
  
  # inner_join of the missing mbz cases with wikidata's mbz
  mbz_from_wiki <- mbz_missing %>% 
    
    inner_join(wiki, by = "deezer_id") %>% 
    filter(!is.na(musicbrainz_id.y)) %>% 
    
    # keep unique matches only
    # REVIEW THIS!
    add_count(deezer_id) %>%
    filter(n == 1) %>%
    
    add_count(musicbrainz_id.y) %>%
    filter(nn == 1) %>%
    
    mutate(musicbrainz_id = musicbrainz_id.y) %>% 
    distinct(deezer_id, musicbrainz_id, .keep_all = T) %>% 
    select(deezer_id, name, musicbrainz_id)
  
  return(mbz_from_wiki)
}



## improve format of logs, export logs to csv
update_rows <- function(all, ..., by = "deezer_id"){
  
  require(dplyr)
  require(logging)
  require(stringr)
  
  patches <- list(...)
  patch_names <- names(patches)
  
  for(i in seq_along(patches)){
    
    loginfo("patching %s to all", patch_names[i])
    
    all <- all %>% 
      rows_update(patches[[i]], by = by)
    
    cleanpop(all)
    loginfo(strrep("-", 40))
    
  }
    
  return(all)
  
}






