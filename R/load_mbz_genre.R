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
  
  require(sjmisc)
  
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
  
  ## filter genres by genre_count and frq
  genre_frq <- frq(genres$mbz_genre, sort.frq = "desc")[[1]]
  genre_frq <- genre_frq %>% 
    select(val, frq) %>% 
    as_tibble()
  
  genres <- genres %>% 
    left_join(genre_frq, by = c(mbz_genre = "val"))
  
  genres <- genres %>% 
    group_by(mbz_artist_id) %>% 
    filter(genre_count == max(genre_count)) %>% 
    filter(frq == max(frq)) %>% 
    ungroup()

  return(genres)

}









