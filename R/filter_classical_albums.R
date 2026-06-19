

filter_classical_albums <- function(album_file, genre_mapping_file){
  
  albums <- load_s3(album_file)
  deezer_genre_mapping <- load_s3(genre_mapping_file)
  
  classical_albums <- albums %>%
    left_join(deezer_genre_mapping, by = "genre_id") %>%
    filter(!is.na(genre)) %>%
    select(album_id, 
           dz_artist_id = "artist_id", 
           album_title, 
           genre) %>%
    filter(genre == "Classique") %>% 
    as_tibble()
  
  # --------------
  
  # load base compositeurs
  comp <- read.csv("data/comp_wide_1706.csv",
                   sep = ";")
  
  comp <- comp %>% 
    as_tibble() %>% 
    mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
    mutate(aliases = str_normalize_klassik(aliases),
           dz_name = str_normalize_klassik(dz_name)) %>% 
    mutate(aliases = paste(dz_name, aliases, sep = "|")) %>% 
    mutate(aliases = gsub("\\s*\\|\\s*", "|", aliases)) %>% 
    select(dz_artist_id, dz_name, aliases) 
  
  
  ## ------------ filter candidate albums (reduce computing time)
  
  pat <- paste(comp$aliases, collapse = "|")
  
  classical_albums <- classical_albums %>%
    mutate(album_title = str_normalize_klassik(album_title)) %>% 
    filter(str_detect(album_title, pat))
  
  ## ------------ match composers with albums
  
  classical_albums <- classical_albums %>%
    select(album_id, album_title) %>%
    crossing(comp) %>%
    filter(
      str_detect(
        album_title,
        regex(
          paste0("\\b(", aliases, ")\\b")
        )
      )
    ) %>% 
    distinct(album_id, dz_artist_id) %>%
    group_by(album_id) %>%
    summarise(
      composer_dz_artist_id = list(unique(dz_artist_id)),
      .groups = "drop"
    )
  
  return(classical_albums)
}


# albums <- load_s3("interim/prod/genres_from_albums.parquet")
# deezer_genre_mapping <- load_s3("interim/dict/deezer_genre_mapping.csv")














