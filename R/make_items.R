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
names_to_conflicts <- function(conflicts, names) {
  
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
  
  return(conflicts_names)
}


### join to scraped names









  