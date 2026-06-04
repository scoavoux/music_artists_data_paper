

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

  # --------------
  comp <- read.csv("data/base_compositeurs.csv",
                   sep = ";")
  
  aliases_to_add <- read.csv("data/aliases_to_add.csv",
                             sep = ";")
  
  aliases_to_add <- aliases_to_add %>% 
    mutate(dz_name = str_to_title(dz_name))
  
  comp <- comp %>% 
    mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
    filter(compositeur == 1) %>% 
    as_tibble() %>%
    distinct(dz_name, .keep_all = T) %>% 
    select(dz_artist_id, dz_name) 
  
  aliases <- comp %>% 
    inner_join(aliases_to_add, by = "dz_name") %>% 
    distinct(dz_artist_id, .keep_all = T) %>% 
    select(dz_artist_id, dz_name = "ent_name") %>% 
    mutate(dz_name = str_to_title(dz_name))
  
  composer_dict <- comp %>% 
    bind_rows(aliases) %>% 
    distinct(dz_name, .keep_all = T)
  
  
  ## ------------ make classical composers
  
  classical_albums <- genre_classical %>%
    
    select(album_id, album_title) %>%
    
    crossing(composer_dict) %>%
    
    filter(
      str_detect(
        album_title,
        fixed(dz_name)
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



comp <- read.csv("data/base_compositeurs.csv",
                 sep = ";")

aliases_to_add <- read.csv("data/aliases_to_add.csv",
                           sep = ";")

dat <- df %>% 
  filter(genre_dz_album_1 == "Classique") %>% 
  select(dz_artist_id, dz_name, n_plays)

aliases_to_add <- aliases_to_add %>% 
  mutate(dz_name = str_to_title(dz_name)) %>% 
  as_tibble() %>% 
  left_join(dat, by = "dz_name") %>% 
  group_by(dz_name) %>% 
  filter(n_plays == max(n_plays)) %>% 
  ungroup() 

# ADD beethoven, ??

comp <- comp %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  filter(compositeur == 1) %>% 
  as_tibble() %>%
  distinct(dz_name, .keep_all = T) %>% 
  select(dz_artist_id, dz_name) 

aliases <- comp %>% 
  inner_join(aliases_to_add, by = "dz_name") %>% 
  distinct(dz_artist_id, .keep_all = T) %>% 
  select(dz_artist_id, dz_name = "ent_name") %>% 
  mutate(dz_name = str_to_title(dz_name))

composer_dict <- comp %>% 
  bind_rows(aliases) %>% 
  distinct(dz_name, .keep_all = T)








