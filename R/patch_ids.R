# find unique name matches between all and refs
# ref_clean <=> reference (contacts/mbz/...), which is anti-joined with all 
# by deezer_id to select missings only
# miss is a subset of all missing ref_id
# matches makes name-based matches between all and ref and retains
# unique matches only, excluding one-to-many and many-to-many matches
# the needed cols are then recoded (ref_id.y to ref_id) and selected
patch_names <- function(all,
                        ref,
                        ref_id,
                        ref_name,
                        all_name) {

  require(logging)
  
  loginfo("allgood")
  
  ref_id   <- rlang::sym(ref_id)
  ref_name <- rlang::sym(ref_name)
  all_name <- rlang::sym(all_name)

  loginfo("allgood")
  
  ## prepare reference table
  ref_clean <- ref %>%
    as_tibble() %>%
    mutate(across(where(is.integer), as.character)) %>%
    select(!!ref_id, !!ref_name) %>%
    filter(!is.na(!!ref_name)) %>%
    anti_join(all, by = setNames(rlang::as_string(ref_id),
                                 rlang::as_string(ref_id)))

  loginfo("allgood")
  
  ## rows in all missing IDs
  miss <- all %>%
    filter(is.na(!!ref_id))

  loginfo("allgood")
  
  ## unique name-based matches
  matches <- miss %>%
    inner_join(ref_clean,
               by = setNames(rlang::as_string(ref_name),
                             rlang::as_string(all_name))) %>%
    add_count(!!all_name, name = "n_all") %>%
    add_count(!!ref_name, name = "n_ref") %>% # breaks because of wiki...

    filter(n_all == 1, n_ref == 1)
  
  loginfo("allgood")
  
  # subset wanted cols
  id_y <- paste0(rlang::as_string(ref_id), ".y")
  
  loginfo("allgood")
  
  matches <- matches %>%
    select(
      !!all_name,
      !!ref_name, ## add ref_name until i solve the wiki names thing
      !!rlang::as_string(ref_id) := !!rlang::sym(id_y),
      deezer_id
    ) 
  
    # ADD ref_name SO IT GETS UPDATED TOO!

  return(matches)
  
  }

  
# --------------------------------------------------------------

# extra function to get some mbz_ids from wikidata
# take missing musicbrainz_ids in all, 

mbz_from_wiki <- function(all, wiki){
  
  mbz_missing <- all %>% 
    filter(is.na(musicbrainz_id)) 
  
  # inner_join of the missing mbz cases with wikidata's mbz
  mbz_from_wiki <- mbz_missing %>% 
    
    inner_join(wiki, by = "deezer_id") %>% 
    filter(!is.na(musicbrainz_id.y)) %>% 
    
    # keep unique matches only
    # REVIEW THIS!
    add_count(deezer_id, name = "n_deezer") %>%
    add_count(musicbrainz_id.y, name = "n_mbz") %>%
    filter(n_deezer == 1, n_mbz == 1) %>% 
    
    mutate(musicbrainz_id = musicbrainz_id.y) %>% 
    distinct(deezer_id, musicbrainz_id, .keep_all = T) %>% 
    select(deezer_id, name, musicbrainz_id)
  
  return(mbz_from_wiki)
}


# --------------------------------------------------

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
    # keep the condition like this for now: adding the other variables adds like 30 cases
    # but unsure about the cases (e.g., there are weird ones with very few albums)
    filter(collection_count > 0) %>% # remove irrelevant artists 
    group_by(contact_name) %>% 
    mutate(col_share = collection_count / sum(collection_count)) %>% 
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



# ----------------------------------------------------------

# wrapper for dplyr::rows_update: takes a list of patches as arg,
# passes them to all with rows_update sequentially, and returns
# the enriched dataset with metrics on the stream share
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








