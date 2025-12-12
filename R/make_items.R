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
    
    select(song_id,
           song_title,
           deezer_feat_id = "artists_ids",
           deezer_id = "artist_id")
  
  return(df)
}


bind_items <- function(items_old, items_new, streams){
  
  # bind items_old and items_new
  # prioritize deezer_id of items_new
  items <- items_new %>% 
    bind_rows(
      items_old %>% 
        anti_join(items_new, by = "song_id")
    )
  
  # remove 3 NAs
  items <-  items %>% 
    filter(!is.na(items$deezer_id))
  
  # separate rows of featurings
  items <- items %>% 
    mutate(deezer_feat_id = map_chr(deezer_feat_id, 
                                    ~ paste(as.integer(.x), 
                                            collapse = ","))) %>% 
    filter(!is.na(deezer_id)) %>% 
    separate_rows(deezer_feat_id, sep = ",") %>% 
    select(song_id,
           song_title,
           deezer_id,
           deezer_feat_id)
  
  ## new col to weight by n featured artists
  items <- items %>%
    group_by(song_id) %>%
    mutate(w_feat = 1 / n_distinct(deezer_feat_id))
  
  ## join to streams and compute weighted popularity
  items <- items %>% 
    inner_join(streams, by = "song_id") %>% 
    mutate(weighted_f_n_play = w_feat * f_n_play)
  
  return(items)
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
           deezer_id_new) %>%
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
    filter(any(deezer_id_old == deezer_id_new)) %>% 
    ungroup() %>% 
    filter(deezer_id_old == deezer_id_new) %>% 
    select(song_id, deezer_id_new)
  
  to_match <- conflicts_names %>% 
    group_by(song_id, deezer_id_old) %>% 
    filter(!any(deezer_id_old  == deezer_id_new)) %>% 
    ungroup() %>% 
    anti_join(removed) %>% 
    distinct(deezer_id_old, deezer_id_new, .keep_all = TRUE) %>% ## took unique pairs !! important
    select(song_id, song_title, deezer_id_old, deezer_id_new, f_n_play, name_old_id, name_new_id)
  
  
  write_csv2(to_match, "data/interim/conflicts_to_match.csv")
  
}























  