### load items_old and/or items_new
make_items <- function(to_remove = to_remove_file,
                       file = "records_w3/items/songs.snappy.parquet") {
  df <- load_s3(file,
                col_select = c("song_id",
                               "artist_id",
                               "artists_ids",
                               "song_title")) %>% 
    anti_join(to_remove, 
              by = "artist_id") %>% 
    
    mutate(deezer_feat_id = map_chr(artists_ids, 
                                    ~ paste(as.integer(.x), 
                                            collapse = ","))) %>% 
    filter(!is.na(artist_id)) %>% 
    separate_rows(deezer_feat_id, sep = ",") %>% 
    select(song_id,
           song_title,
           deezer_id = "deezer_feat_id")
  
  return(df)
}
  

### make conflicted items
make_conflict_items <- function(items_old, items_new, streams) {
  
  df <- items_old %>% 
    inner_join(items_new, 
               by = "song_id", 
               suffix = c("_old", "_new")) %>%
    group_by(song_id, deezer_id_old) %>%
    mutate(shared_deezer_id = any(deezer_id_old == deezer_id_new)) %>% # boolean col "any shared id"
    group_by(song_id) %>%
    mutate(keep_song = any(!shared_deezer_id), # boolean column "keep song"
           deezer_id_old = as.integer(deezer_id_old),
           deezer_id_new = as.integer(deezer_id_new)) %>% 
    ungroup() %>%
    filter(keep_song) %>% # drop if FALSE
    select(song_id,
           song_title = song_title_old, # keep old only because song title never changes
           deezer_id_old,
           deezer_id_new) %>% # to int for later operations
    arrange(song_id, song_title) %>% 
    inner_join(streams, by = "song_id")
  
  return(df)
}
  

#### join to names
names_to_conflicts <- function(conflicts, names, new_names) {
  
  names_old <- names %>% 
    select(deezer_id_old = artist_id, 
           name_old_id = name)
  
  names_new <- names %>% 
    select(deezer_id_new = artist_id, 
           name_new_id = name)
  
  conflicts_names <- conflicts %>%
    left_join(names_old, by = "deezer_id_old") %>% 
    left_join(names_new, by = "deezer_id_new") %>% 
    arrange(desc(f_n_play)) %>% 
    select(song_title, 
           name_old_id, 
           name_new_id,
           deezer_id_old, 
           deezer_id_new,
           n_play, 
           f_n_play, 
           song_id)
  
  ## join names scraped from deezer API
  conflicts_names <- conflicts_names %>%
    left_join(new_names, by = "deezer_id_new") %>%
    mutate(name_new_id = coalesce(name_new_id, name),
           deezer_match = ifelse(deezer_id_old == deezer_id_new, TRUE, FALSE)) %>%
    select(-name)
  
  return(conflicts_names)
}


### join to scraped names


### filter conflicts to match
filter_conflicts_to_match <- function(conflicts_names) {
  
  removed <- conflicts_names %>% 
    group_by(song_id, deezer_id_old) %>%
    filter(any(deezer_id_old  == deezer_id_new)) %>% 
    ungroup() %>% 
    filter(deezer_id_old  == deezer_id_new) %>% 
    select(song_id, deezer_id_new)
  
  to_match <- conflicts_names %>% 
    group_by(song_id, deezer_id_old) %>% 
    filter(!any(deezer_id_old  == deezer_id_new)) %>% 
    ungroup() %>% 
    anti_join(removed) %>% 
    distinct(deezer_id_old, deezer_id_new, .keep_all = TRUE) %>% 
    select(song_id, song_title, deezer_id_old, deezer_id_new, f_n_play, name_old_id, name_new_id)
  
  
  write_csv2(to_match, "data/interim/conflicts_to_match.csv")
  
}


### filter conflicts to match
resolve_conflicts <- function(conflicts_names, matched_names) {
  
  
  

}


























  