  

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
  
  
    
    
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
