

load_dz_genre_album <- function(album_file, genre_mapping_file){
  
  albums <- load_s3(album_file)
  
  deezer_genre_mapping <- load_s3(genre_mapping_file)

  albums <- albums %>% 
    left_join(deezer_genre_mapping, by = "genre_id") %>% 
    filter(!is.na(genre)) %>% 
    filter(record_type != "compilation") %>% 
    rename(dz_artist_id = "artist_id")

  main_genres <- albums %>%
    mutate(
      weight = log(fans + 1)
    ) %>%
    
    # 1. Choose release type per artist
    group_by(dz_artist_id) %>%
    mutate(
      chosen_type = case_when(
        any(record_type == "album") ~ "album",
        any(record_type == "ep")    ~ "ep",
        TRUE                        ~ "single"
      )
    ) %>%
    ungroup() %>%
    
    # 2. Keep only chosen type
    filter(record_type == chosen_type) %>%
    
    # 3. Aggregate weights per genre
    group_by(dz_artist_id, genre) %>%
    summarise(
      genre_weight = sum(weight, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    
    # 4. For each artist, extract top 2 genres into columns
    group_by(dz_artist_id) %>%
    arrange(desc(genre_weight), genre, .by_group = TRUE) %>%
    summarise(
      genre_dz_album_1 = first(genre),
      genre_dz_album_2 = dplyr::nth(genre, 2),
      .groups = "drop"
    )
  
  return(main_genres)
}


  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
