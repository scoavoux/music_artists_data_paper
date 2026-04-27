

make_genres_from_albums <- function(album_file, genre_mapping_file){
  
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
      genre_1 = first(genre),
      genre_2 = dplyr::nth(genre, 2),
      .groups = "drop"
    )
  
  return(main_genres)
}





make_dz_genres <- function(){
  
  # ------------------- DEEZER MAIN GENRE
  genre_dz_main <- load_s3("records_w3/items/artists_data.snappy.parquet") 
  
  genre_dz_main <- genre_dz_main %>% 
    mutate(dz_artist_id = as.character(artist_id),
           dz_name = name,
           genre_dz_main = main_genre) %>% 
    filter(!is.na(genre_dz_main)) %>% 
    select(dz_artist_id, genre_dz_main)
  
  # ---------------- DEEZER GENRE FROM ALBUMS
  genres_dz_albums <- read_csv("data/genres_from_deezer_albums.csv")
  
  genres_dz_albums <- genres_dz_albums %>% 
    mutate(dz_artist_id = as.character(artist_id),
           genre_dz_albums = genre) %>% 
    select(dz_artist_id, genre_dz_albums) %>% 
    as_tibble()
  
  # ------------------- JOIN
  genre_dz <- full_join(genre_dz_main, genres_dz_albums, by = "dz_artist_id")
  
  return(genre_dz)
}
  
  
make_sc_genre <- function(){
  
  # --------------- SENSCRITIQUE TAGS
  
  album_tags <- load_s3("senscritique/albums_tags.csv")
  tags_meaning <- load_s3("senscritique/tags_meaning.csv")
  contacts_albums_list <- load_s3("senscritique/contacts_albums_link.csv")
  
  genre_sc_tags <- album_tags %>% 
    as_tibble() %>% 
    left_join(tags_meaning, by = "genre_tag_id") %>% 
    left_join(contacts_albums_list, by = "product_id") %>% 
    
    #mutate(genre = recode_vars(genre, .source)) %>% 
    filter(!is.na(contact_id), !is.na(genre)) %>%
    
    # importance of genres for each artist
    count(contact_id, product_id, genre) %>% 
    mutate(f = n / sum(n), .by = product_id) %>%
    summarise(f = sum(f), .by = c(contact_id, genre)) %>% 
    mutate(f = f / sum(f), .by = contact_id) %>% 
    
    arrange(contact_id, desc(f)) %>% 
    slice(1, .by = contact_id) %>% 
    filter(f > .3) %>% 
    
    select(contact_id, genre) %>% 
    
    mutate(sc_artist_id = as.character(contact_id),
           genre_sc_tags = genre) %>% 
    select(sc_artist_id, genre_sc_tags)
  
  return(genre_sc_tags)
  
}
  
  
    
    
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
