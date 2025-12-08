add_missing_names <- function(conflicts_names) {
  
  
  new_names_to_scrape <- conflicts_names %>%
    filter(is.na(name_new_id)) %>% 
    select(deezer_id_new, name_old_id)
  

  get_artist_name <- function(artist_id){
    url <- str_glue("https://api.deezer.com/artist/{artist_id}")
    page <- RETRY("GET", url) |> content(as = "parsed")
    
    res <- tibble(deezer_id = page$id,
                  name = page$name)
    return(res)
  }
  
  # remplacer artists par un tibble avec artist_id = les id des artists dont le nom manque. 
  # à l'initialisation, ajouter une colonne scraped = FALSE pour tout le monde
  # (permet de faciliter la relance quand la boucle plante)
  
  artists <- new_names_to_scrape %>% 
    rename(artist_id = deezer_id_new) %>% 
    mutate(scraped = FALSE)
  
  results <- vector("list", length = nrow(artists))
  
  # time control (limite API: 50 requêtes toutes les 5 secondes
  start <- now()
  
  for(i in 1:nrow(artists)) {
    
    if(artists$scraped[i]) next
    results[[i]] <- get_artist_name(artists$artist_id[i])
    artists$scraped[i] <- TRUE
    
    # time control (limite API: 50 requêtes toutes les 5 secondes
    if(i %% 50 == 0) {
      while(now() < (start+duration(6))){
        Sys.sleep(.1)
      }
      print(str_glue("{i} artists done, {nrow(artists)-i} to go"))
      start <- now()
    }
    
    
  }
  
  new_artists_names_from_api <- bind_rows(results) %>% 
      rename(deezer_id_new = deezer_id) %>% 
    distinct(deezer_id_new, .keep_all = TRUE)
  
  conflicts_names_full <- conflicts_names %>%
    left_join(new_artists_names_from_api, by = "deezer_id_new") %>%
    mutate(name_new_id = coalesce(name_new_id, name),
           deezer_match = ifelse(deezer_id_old == deezer_id_new, TRUE, FALSE)) %>%
    select(-name)
  
  conflicts_names_full <- as_tibble(conflicts_names_full)
  
  return(conflicts_names_full)
}



# write_csv(new_artists_names_from_api, "data/interim/new_artists_names_from_api.csv")

  
  
  
  
  
  