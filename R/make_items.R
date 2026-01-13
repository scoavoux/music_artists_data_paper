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


bind_items <- function(items_old, items_new, streams, names){
  
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
   inner_join(streams, by = "song_id") 

  ## add deezer names to debug joins with other ids
  items <- items %>% 
    left_join(names, by = "deezer_feat_id") # CHANGED TO DEEZER_FEAT_ID
  
  return(items)
}


bind_names <- function(file_1, file_2){
  
  names <- load_s3(file = file_1)
  scraped_names <- load_s3(file = file_2)
  
  names <- names %>% 
    mutate(deezer_feat_id = as.character(artist_id)) %>% # CHANGED TO DEEZER_FEAT_ID
    select(deezer_feat_id, name)
  
  scraped_names <- scraped_names %>% 
    mutate(deezer_feat_id = as.character(deezer_id.new))
  
  names <- names %>% 
    bind_rows(scraped_names)
  
  return(names)

}

## unique artists for now --- because of f_n_play
group_items_by_artist <- function(items){
  
  artists <- items %>% 
    ungroup() %>% 
    mutate(deezer_feat_id = as.character(deezer_feat_id),
           w_n_play = w_feat * n_play, # weight n plays by feat
           w_f_n_play = w_n_play / sum(w_n_play)) %>% # compute weighted f_n_play
    group_by(deezer_feat_id) %>% 
    summarise(name = first(name),
              pop = sum(w_f_n_play),
              .groups = "drop") %>% 
    arrange(desc(pop))
  
  return(artists)
}











  