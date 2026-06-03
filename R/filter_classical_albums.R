

filter_classical_albums <- function(album_file, genre_mapping_file){
  
  require(stringr)
  
  albums <- load_s3(album_file)
  deezer_genre_mapping <- load_s3(genre_mapping_file)

  genre_classical <- albums %>%
    left_join(deezer_genre_mapping, by = "genre_id") %>%
    filter(!is.na(genre)) %>%
    select(album_id, 
           dz_artist_id = "artist_id", 
           album_title, 
           genre) %>%
    filter(genre == "Classique") %>% 
    as_tibble()

  # ---------------- make dictionary of composer aliases
  
  composer_dict <- tibble(dz_artist_id = c(1900, 5695, 6144, 5176),
                          
                          composer_alias = c("Bach", 
                                             "Mozart", 
                                             "Beethoven", 
                                             "Debussy")
  )
  
  ## ------------ make classical composers
  
  classical_albums <- genre_classical %>%
    
    select(album_id, album_title) %>%
    
    crossing(composer_dict) %>%
    
    filter(
      str_detect(
        str_to_lower(album_title),
        fixed(str_to_lower(composer_alias))
      )
    ) %>%
    
    distinct(
      album_id,
      dz_artist_id
    ) %>%
    
    group_by(album_id) %>%
    
    summarise(
      composer_dz_artist_id = list(unique(dz_artist_id)),
      .groups = "drop"
    )
  
  return(classical_albums)
}



















