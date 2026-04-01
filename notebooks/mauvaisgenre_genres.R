make_genres_data <- function(.source = "deezer_editorial_playlists", senscritique_mb_deezer_id){
  require(tidyverse)
  s3 <- initialize_s3()
  if(.source == "deezer_editorial_playlists"){
    f <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/artists_genre_weight.csv")
    genres <- f$Body %>% rawToChar() %>% read_csv()
    genres <- genres %>% 
      select(-artist_name) %>% 
      filter(n_playlists_used > 5) %>% 
      pivot_longer(african:soulfunk, names_to = "genre") %>% 
      # We use the main genre with at least 33% of playlists
      filter(value > .33) %>%
      arrange(artist_id, desc(value)) %>% 
      slice(1, .by = artist_id) %>% 
      select(-n_playlists_used, -value) %>% 
      mutate(genre = recode_vars(genre, .source))
    # Check coverage of genre definition
    # user_artist_peryear %>% 
    #   filter(!is.na(artist_id)) %>% 
    #   left_join(genres) %>% 
    #   mutate(na = is.na(genre)) %>% 
    #   group_by(na) %>% 
    #   summarise(n=sum(n_play))
    # With .4 threshold, 70% of plays; with .3, 79%
    
  } else if(.source == "deezer_maingenre"){
    s3$download_file(Bucket = "scoavoux", 
                     Key = "records_w3/items/artists_data.snappy.parquet",
                     Filename = "data/temp/artists_data.snappy.parquet")
    artists <- read_parquet("data/temp/artists_data.snappy.parquet", col_select = 1:3)
    artists <- artists %>% 
      filter(!is.na(main_genre))
    genres <- artists %>% 
      select(artist_id, genre = "main_genre") %>% 
      mutate(genre = recode_vars(genre, .source))
    # TODO:Collapse
    
  } else if(.source == "senscritique_tags"){
    f <- s3$get_object(Bucket = "scoavoux", Key = "senscritique/albums_tags.csv")
    album_tags <- f$Body %>% rawToChar() %>% read_csv()
    f <- s3$get_object(Bucket = "scoavoux", Key = "senscritique/tags_meaning.csv")
    tags_meaning <- f$Body %>% rawToChar() %>% read_csv()
    f <- s3$get_object(Bucket = "scoavoux", Key = "senscritique/contacts_albums_link.csv")
    contacts_albums_list <- f$Body %>% rawToChar() %>% read_csv()
    rm(f)
    
    genres <- left_join(album_tags, tags_meaning) %>% 
      left_join(select(contacts_albums_list, -contact_subtype_id), relationship = "many-to-many") %>% 
      left_join(select(senscritique_mb_deezer_id, -mbid), relationship = "many-to-many") %>% 
      mutate(genre = recode_vars(genre, .source)) %>% 
      filter(!is.na(artist_id), !is.na(genre)) %>%
      count(artist_id, product_id, genre) %>% 
      mutate(f = n/sum(n), .by = product_id) %>% 
      summarise(f = sum(f), .by = c(artist_id, genre)) %>% 
      mutate(f = f / sum(f), .by = artist_id) %>% 
      arrange(artist_id, desc(f)) %>% 
      slice(1, .by = artist_id) %>% 
      filter(f > .3) %>% 
      select(artist_id, genre)
    # TODO: collapse
    
  } else if(.source == "genres_from_deezer_albums"){
    genres <- read_csv("data/genres_from_deezer_albums.csv") %>% 
      mutate(genre = recode_vars(genre, .source))
  } else if(.source == "musicbrainz_tags"){
    # THERE IS SOMETHING WRONG PROBABLY IN MBID. FOR INSTANCE COCTEAU TWINS is
    # "salsa choke" and Death Cab for Cutie is "Non-Music"
    # f <- s3$get_object(Bucket = "scoavoux", Key = "senscritique/mbid_deezerid_pair.csv")
    # mb_dz <- f$Body %>% rawToChar() %>% read_csv()
    # mb_dz <- mb_dz %>% filter(!is.na(mbid))
    # f <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_genre.csv")
    # mb_genres <- f$Body %>% rawToChar() %>% read_csv()
    # mb_genres <- mb_genres %>% filter(!is.na(mbid))
    # mbg <- mb_dz %>% 
    #   inner_join(mb_genres) %>% 
    #   arrange(mbid, desc(count)) %>% 
    #   slice(1, .by = mbid)
  }
  return(genres)
}

# Merge genres ------
## Takes a list of tibbles describing artist genres
## and return a list of unique genre attribution for each artist
merge_genres <- function(...){
  require(tidyverse)  
  # order of arguments is the order of priority of databases
  genres <- bind_rows(...) %>% 
    slice(1, .by = artist_id)
  return(genres)
}