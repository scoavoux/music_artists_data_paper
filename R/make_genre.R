# make genre_dz_album
# aggregated from deezer album genres
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

# make genre_mbz_album
# aggregated from musicbrainz album genres
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


# make genre_mbz_artist
# mbz genre tags at the artist level
load_mbz_genre_artist <- function(file){
  
  genres_raw <- load_s3(file)
  
  genres <- genres_raw %>%
    as_tibble() %>%
    rename(mbz_artist_id = artist_mbid,
           mbz_genre = genre_name) %>%
    filter(mbz_genre != "")
  
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


  
  

  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
