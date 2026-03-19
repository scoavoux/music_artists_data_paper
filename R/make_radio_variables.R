count_radio_plays <- function(file){
  
  radio <- load_s3(file)
  
  radio <- radio %>%
    as_tibble() %>% 
    mutate(dz_artist_id = as.character(artist_id)) %>% 
    filter(!is.na(dz_artist_id)) %>% 
    count(dz_artist_id, radio) %>% 
    mutate(
      n_public = if_else(radio %in% c("France Musique", "France Inter", "Fip"), n, 0)
    ) %>% 
    group_by(dz_artist_id) %>% 
    summarize(radio_n_plays = sum(n),
              radio_n_plays_public_stations = sum(n_public))
  
  return(radio)
}



