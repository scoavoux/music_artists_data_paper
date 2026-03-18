### load items_old and/or items_new
make_dz_songs <- function(to_remove = to_remove_file,
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
           dz_artist_feat_id = "artists_ids",
           dz_artist_id = "artist_id")
  
  return(df)
}


bind_dz_songs <- function(dz_songs_old, dz_songs_new, #dz_streams, 
                          dz_names){
  
  # bind items_old and items_new
  # prioritize deezer_id of items_new
  songs <- dz_songs_new %>% 
    bind_rows(
      dz_songs_old %>% 
        anti_join(dz_songs_new, by = "song_id")
    )
  
  # remove 3 NAs
  songs <-  songs %>% 
    filter(!is.na(songs$dz_artist_id))
  
  # separate rows of featurings
  songs <- songs %>% 
    mutate(dz_artist_feat_id = map_chr(dz_artist_feat_id, 
                                    ~ paste(as.integer(.x), 
                                            collapse = ","))) %>% 
    filter(!is.na(dz_artist_id)) %>% 
    separate_rows(dz_artist_feat_id, sep = ",") %>% 
    select(song_id,
           song_title,
           dz_artist_id,
           dz_artist_feat_id)
  
  ## new col to weight by n featured artists
  songs <- songs %>%
    group_by(song_id) %>%
    mutate(w_feat = 1 / n_distinct(dz_artist_feat_id))
  
  ## join to streams
  #songs <- songs %>% 
   #inner_join(dz_streams, by = "song_id") ## inner_join appropriate??

  ## add deezer names to debug joins with other ids
  songs <- songs %>% 
    left_join(dz_names, by = "dz_artist_feat_id") 
  
  return(songs)
}


bind_dz_names <- function(file_1, file_2){
  
  names <- load_s3(file = file_1)
  scraped_names <- load_s3(file = file_2)
  
  names <- names %>% 
    mutate(dz_artist_feat_id = as.character(artist_id),
           dz_name = name) %>% # CHANGED TO DEEZER_FEAT_ID
    select(dz_artist_feat_id, dz_name)
  
  scraped_names <- scraped_names %>% 
    mutate(dz_artist_feat_id = as.character(deezer_id.new),
           dz_name = name) %>% 
    select(-c(deezer_id.new, name)) %>% 
    as_tibble()
  
  names <- names %>% 
    bind_rows(scraped_names)
  
  return(names)

}


group_songs_by_artist <- function(dz_songs, dz_stream_data){
  
  dz_artists <- dz_songs %>% 
    ungroup() %>% 
    mutate(dz_artist_id = as.character(dz_artist_feat_id)) %>%  # ATTENTION: renaming feat_id to id here!!
    group_by(dz_artist_id) %>% 
    summarise(dz_name = first(dz_name),
              .groups = "drop") %>% 
    
    # NEW 18/03: ADD POP HERE!
    inner_join(dz_stream_data, by = "dz_artist_id") %>% 
    mutate(dz_stream_share = n_plays / sum(n_plays, na.rm = T) * 100) %>%  # TEMPORARY
    
    arrange(desc(dz_stream_share))
  
  return(dz_artists)
}


  
  
  
  
  
  
  
  
  