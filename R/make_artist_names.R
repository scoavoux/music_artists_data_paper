make_artists_names <- function(artist_names_and_aliases){
  
  require(tidyverse)
  
  artist_names <- artist_names_and_aliases %>% 
    mutate(type = factor(type, levels = c("deezername", "name", "alias"))) %>% 
    arrange(type) %>% 
    slice(1, .by = artist_id) %>% 
    select(artist_id, name)
  
  return(artist_names)
}