library(jsonlite)

url <- "https://api.deezer.com/genre"
parsed <- fromJSON(url)

deezer_genre_mapping <- parsed$data

deezer_genre_mapping <- deezer_genre_mapping %>% 
  select(genre_id = "id", genre = "name") %>% 
  filter(genre_id != 0) %>% 
  as_tibble()

l <- unique(albums$genre_id)
newgenres <- l[!l %in% deezer_genre_mapping$genre_id]

test <- albums %>% 
  filter(genre_id %in% newgenres) %>% 
  filter(genre_id != -1)

table(test$genre_id)

newgenres_mapping <- tibble(genre_id = newgenres,
                            genre = c(NA, "singer & songwriter", "disco",
                                      "soul", "sports", "contemporary soul",
                                      NA, "corridos", "technology", "ranchera",
                                      "comedy", "norteño", "international pop",
                                      "contemporary r&b", "bolero",
                                      "film scores", "spirituality & religion",
                                      "oldschool r&b", "kids & family", "grime",
                                      "education", "variété internationale",
                                      "old school soul", "storytelling",
                                      "techno/house", "tv soundtracks"))

deezer_genre_mapping <- deezer_genre_mapping %>% 
  bind_rows(newgenres_mapping) %>% 
  distinct()
