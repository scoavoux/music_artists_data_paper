load_mbz_genre_album <- function(df, file){
  
  genres_raw <- load_s3(file)
  
  genres <- genres_raw %>% 
    as_tibble() %>% 
    rename(mbz_artist_id = "artist_mbid") %>% 
    filter(album_type == "Studio") %>% 
    filter(genre_name != "") %>% 
    select(-album_type)
  
  genres <- df %>% 
    inner_join(genres, by = "mbz_artist_id") %>% 
    select(mbz_artist_id, 
           dz_name, 
           mbz_genre = "genre_name", 
           n_releases, 
           release_group_mbid,
           n_plays_share)
  
  return(genres)
  
}



load_mbz_genre_artist <- function(df, file){
  
  genres_raw <- load_s3(file)
  
  genres <- genres_raw %>% 
    as_tibble() %>% 
    rename(mbz_artist_id = "artist_mbid") %>% 
    filter(genre_name != "")
  
  genres <- df %>% 
    inner_join(genres, by = "mbz_artist_id") %>% 
    select(mbz_artist_id, 
           dz_name, 
           mbz_genre = "genre_name", 
           genre_count, 
           n_plays_share)

  return(genres)

}

tar_load(mbz_genre_artist)
mbz_genre_artist
tar_load(mbz_genre_album)
mbz_genre_album












