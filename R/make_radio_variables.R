count_radio_plays <- function(file){
  
  radio <- load_s3(file)
  
  radio <- radio %>%
    as_tibble() %>% 
    rename(dz_artist_id = "artist_id") %>% 
    filter(!is.na(dz_artist_id)) %>% 
    count(dz_artist_id, radio) %>% 
    mutate(
      leg = if_else(radio %in% c("France Musique", "France Inter", "Fip"), n, 0)
    ) %>% 
    group_by(dz_artist_id) %>% 
    summarize(radio_total = sum(n),
              radio_leg = sum(leg))
  
  return(radio)
}





