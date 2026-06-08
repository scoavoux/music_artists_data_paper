load_mbz_genre_album <- function(file){
  
  genres_raw <- load_s3(file)
  
  genres_raw <- genres_raw %>%
    as_tibble() %>%
    rename(mbz_artist_id = artist_mbid,
           mbz_genre = genre_name) %>%
    filter(mbz_genre != "")
  
  # aggregate album genres to artist level
  artist_genres <- genres_raw %>%
    group_by(mbz_artist_id, mbz_genre) %>%
    add_count(mbz_genre) %>% 
    summarise(
      genre_count = n(),
      .groups = "drop"
    )
  
  # overall genre popularity
  genre_frq <- frq(artist_genres$mbz_genre, sort.frq = "desc")[[1]] %>%
    select(val, frq) %>%
    as_tibble()
  
  artist_genres <- artist_genres %>%
    left_join(
      genre_frq,
      by = c("mbz_genre" = "val")
    )
  
  # select top 2 genres per artist
  artist_genres_top2 <- artist_genres %>%
    group_by(mbz_artist_id) %>%
    arrange(
      desc(genre_count),  # primary ranking
      desc(frq),          # tie breaker
      .by_group = TRUE
    ) %>%
    slice_head(n = 2) %>%
    mutate(rank = row_number()) %>%
    ungroup()
  
  # wide format
  artist_genres_wide <- artist_genres_top2 %>%
    select(
      mbz_artist_id,
      rank,
      mbz_genre
    ) %>%
    pivot_wider(
      names_from = rank,
      values_from = mbz_genre,
      names_prefix = "genre_mbz_album_"
    ) %>% 
    select(mbz_artist_id, starts_with("genre_mbz_album_"))
  
}


load_mbz_genre_artist <- function(file){
  
  genres_raw <- load_s3(file)
  
  genres <- genres_raw %>%
    as_tibble() %>%
    rename(mbz_artist_id = artist_mbid,
           mbz_genre = genre_name) %>%
    filter(mbz_genre != "")
  
  # join to df for tests --> reuse if more tests needed
  # genres <- df %>%
  #   inner_join(genres, by = "mbz_artist_id") %>%
  #   select(
  #     mbz_artist_id,
  #     dz_name,
  #     mbz_genre,
  #     genre_count,
  #     n_plays_share
  #   )
  
  # overall genre frequency
  genre_frq <- frq(genres$mbz_genre, sort.frq = "desc")[[1]] %>%
    select(val, frq) %>%
    as_tibble()
  
  genres <- genres %>%
    left_join(
      genre_frq,
      by = c("mbz_genre" = "val")
    )
  
  # rank genres within artist
  genres <- genres %>%
    group_by(mbz_artist_id) %>%
    arrange(
      desc(genre_count),  # primary criterion
      desc(frq),          # tie breaker
      .by_group = TRUE
    ) %>%
    slice_head(n = 2) %>%
    mutate(rank = row_number()) %>%
    ungroup()
  
  # wide format
  genres_wide <- genres %>%
    select(
      mbz_artist_id,
      rank,
      mbz_genre
    ) %>%
    pivot_wider(
      names_from = rank,
      values_from = mbz_genre,
      names_prefix = "genre_mbz_artist_"
    ) %>% 
    select(mbz_artist_id, starts_with("genre_mbz_artist_"))
  
  return(genres_wide)
}


### ADD COMPLETE TABLES FOR MBZ GENRE ARTIST AND ALBUM

















