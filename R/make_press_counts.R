# DISCLAIMER: this workflow is **temporary**.
# separate targets are made for aliases to add and names to drop because
# we might update these later, and until then it is best to not integrate them. 
# importantly, both aliases and names to drop lists are made looking at 

# 1. the export of the entities WITHOUT a match that have 
# the highest mentions (currently n > 30),

# 2. the export of the highest outliers (currently n > 30) 
# among entities WITH a match. outliers <=> abs(log(n_plays / name_count)

# for FUTURE WORK on this: just take exports built into "press_counts"
# and continue hand-coding them


## clean press entities, i.e. remove short strings, 
##normalize name and sum name_counts of homonyms
clean_press_ents <- function(file){
  
  ents <- load_s3(file)

  # prepare ents
  ents <- ents %>% 
    as_tibble() %>% 
    mutate(ent_name = str_normalize(ent_name)) %>% 
    
    group_by(ent_name) %>% 
    # summarize name counts for name duplicates
    mutate(press_n_mentions = sum(name_count),
           press_n_mentions_lefigaro = sum(name_count_lefigaro),
           press_n_mentions_lemonde = sum(name_count_lemonde),
           press_n_mentions_liberation = sum(name_count_liberation),
           press_n_mentions_telerama = sum(name_count_telerama)) %>% 
    ungroup() %>% 
    
    distinct(ent_name, .keep_all = T) %>% # keep only unique ents
    filter(str_length(ent_name) > 2) %>% # remove short ents
    
    select(c(name_id, ent_name, article_id, 
             starts_with("press_n_mentions")))
  
    return(ents)
}


## make a df of the aliases to add the press_n_mentionss of to real artists
list_aliases <- function(file1, file2, artists){
  
  # prepare all_final
  artists <- artists %>% 
    filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% # complete cases
    mutate(dz_name = str_normalize(dz_name)) %>% # normalize name
    group_by(dz_name) %>% 
    
    mutate(keep = ifelse(n_plays == max(n_plays), 
                         TRUE, 
                         FALSE)) %>% # deduplicate CHANGE: SUM OF press_n_mentions INSTEAD!!
    filter(keep == TRUE) %>% 
    ungroup() %>% 
    select(dz_name, n_plays, dz_artist_id)
  
  ents_without_match <- load_s3(file1) 
  press_outliers_checked <- load_s3(file2) 
  
  added_aliases <- ents_without_match %>% 
    filter(is_alias == 1 & dz_name != "") %>% 
    left_join(artists, by = "dz_name") %>%
    select(ent_name, dz_name,
           starts_with("press_n_mentions")) %>% 
    as_tibble()
  
  recoded_aliases <- press_outliers_checked %>% 
    filter(is_alias == 1) %>% 
    as_tibble() %>% 
    select(ent_name, dz_name,
           starts_with("press_n_mentions"))
  
  aliases <- rbind(added_aliases, recoded_aliases)
  
  return(aliases)
}


# make a df of entities which get NA either because 
# they are non-musical artists or homonyms of famous artists
list_entities_to_drop <- function(file){

  press_outliers_checked <- load_s3(file)
  
  # non-artists or ambiguous names
  wrong_name <- press_outliers_checked %>% 
    filter(to_drop == 1)
  wrong_name <- wrong_name$ent_name
  
  # homonyms of famous artists (e.g., "beethoven")
  wrong_alias <- press_outliers_checked %>% 
    filter(is_alias == 1)
  wrong_alias <- wrong_alias$ent_name
  
  names_to_drop <- c(wrong_name, wrong_alias)
  
  # assign NA on both metrics to update
  names_to_drop <- tibble(dz_name = names_to_drop,
                          press_n_mentions = NA,
                          press_n_mentions_lefigaro = NA,
                          press_n_mentions_lemonde = NA,
                          press_n_mentions_liberation = NA,
                          press_n_mentions_telerama = NA)
  return(names_to_drop)
}


## join press press_n_mentionss to all_final and export the 
## 2 csv files to be hand-coded
count_names_press <- function(artists, press_named_entities, min_n_mentions){
  
  # prepare all_final
  artists <- artists %>% 
    filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% # complete cases
    mutate(dz_name = str_normalize(dz_name)) %>% # normalize name
    group_by(dz_name) %>% 
    mutate(keep = ifelse(n_plays == max(n_plays), 
                         TRUE, 
                         FALSE)) %>% # deduplicate
    filter(keep == TRUE) %>% 
    ungroup() %>%
    add_count(dz_name) %>% 
    filter(n == 1) %>% # remove 15 (insignificant) duplicates tied on popularity
    select(dz_name, n_plays, dz_artist_id)
  
    # ------------- 1. match on name in dz_names
  press_name_counts <- artists %>% 
    left_join(press_named_entities, by = c(dz_name = "ent_name")) %>% 
    mutate(corr_pop = abs(log(n_plays / press_n_mentions))) %>% # MIX WITH ARTICLE_COUNT?
    arrange(desc(press_n_mentions))
  
  # ------------- 2.  export (biggest?) non-matched ents + biggest outliers
  # 1. non-matched entities
  ents_without_match <- press_named_entities %>% 
    anti_join(press_name_counts, by = c(ent_name = "dz_name")) %>% 
    filter(press_n_mentions >= min_n_mentions)
  
  write_s3(ents_without_match, file = "press_files/ents_without_match.csv")
  
  # 2. outliers
  press_counts_outliers <- press_name_counts %>% 
    filter(press_n_mentions >= min_n_mentions) %>% 
    arrange(desc(corr_pop))
  
  write_s3(press_counts_outliers, file = "press_files/press_counts_outliers.csv")
  
  press_name_counts <- press_name_counts %>% 
    select(-c(article_id, corr_pop))
  
  return(press_name_counts)
}


## update press_name_counts with hand-coded aliases and cases to drop??
update_press_names <- function(press_name_counts, aliases_to_add, entities_to_drop){
  
  totals <- bind_rows(
    press_name_counts %>%
      select(dz_name, starts_with("press_n_mentions")),
    
    aliases_to_add %>%
      select(dz_name, starts_with("press_n_mentions"))
  ) %>%
    group_by(dz_name) %>%
    summarize(
      across(starts_with("press_n_mentions"), ~sum(.x, na.rm = TRUE)),
      .groups = "drop"
    )
  
  # apply drop logic
  totals <- totals %>% 
    anti_join(entities_to_drop, by = "dz_name") %>% 
    bind_rows(entities_to_drop)
  
  upd_press_name_counts <- press_name_counts %>%
    select(-starts_with("press_n_mentions")) %>%
    left_join(totals, by = "dz_name") %>% 
    select(-name_id)
  
  return(upd_press_name_counts)
}


## integrate press name counts to all_final, making sure only the
## identified wrong cases get NA --- all others == 0
make_press_counts <- function(artists, upd_press_name_counts){
  
  upd_press_name_counts <- upd_press_name_counts %>% 
    select(-c(dz_name, n_plays))
  
  artists <- artists %>% 
    left_join(upd_press_name_counts, by = "dz_artist_id") %>%
    mutate(
      matched = dz_artist_id %in% upd_press_name_counts$dz_artist_id,
      across(starts_with("press_n_mentions"),
             ~ ifelse(!matched, 0, .))
    ) %>% # make sure only the hand-coded NAs are NA, all others == 0
    select(-matched)
  
  return(artists)
}












