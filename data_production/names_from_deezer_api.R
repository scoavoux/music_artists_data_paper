
library(tidyverse)
library(httr)
library(lubridate)

get_artist_name <- function(artist_id){
  url <- str_glue("https://api.deezer.com/artist/{artist_id}")
  page <- RETRY("GET", url) |> 
    content()
  res <- tibble(deezer_id = page$id,
                name = page$name)
  return(res)
}
# remplacer artists par un tibble avec artist_id = les id des artists dont le nom manque. 
# à l'initialisation, ajouter une colonne scrapped = FALSE pour tout le monde
# (permet de faciliter la relance quand la boucle plante)

artists <- tibble(artist_id = c(1, 2), scrapped = FALSE)
results <- vector("list", length = nrow(artists))
# time control (limite API: 50 requêtes toutes les 5 secondes
start <- now()
for(i in 1:nrow(artists)){
  if(artists$scrapped[i]) next
  results[[i]] <- get_artist_name(artists$artist_id[i])
  artists$scrapped[i] <- TRUE
  
  # time control (limite API: 50 requêtes toutes les 5 secondes
  if(i %% 50 == 0) {
    while(now() < (start+duration(6))){
      Sys.sleep(.1)
    }
    print(str_glue("{i} artists done, {nrow(artists)-i} to go"))
    start <- now()
  }

    
}
artists_names_from_api <- bind_rows(results)
