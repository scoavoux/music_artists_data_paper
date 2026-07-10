# albums <- load_s3("interim/prod/genres_from_albums.parquet")
# deezer_genre_mapping <- load_s3("interim/dict/deezer_genre_mapping.csv")


install.packages("stringstatic")
library(stringstatic)
library(stringi)

str_norm_klassik <- function(str){
  
  library(stringi)
  library(stringstatic)
  library(stringr)
  
  str <- str %>% 
    
    str_to_lower() %>%  
    
    stri_trans_general("Latin-ASCII") %>% # rm accents
    
    str_squish() # trim + remove extra spaces
  
  return(str)
}

albums <- load_s3(album_file)
deezer_genre_mapping <- load_s3(genre_mapping_file)

classical_albums <- albums %>%
  left_join(deezer_genre_mapping, by = "genre_id") %>%
  filter(!is.na(genre)) %>%
  select(album_id, 
         dz_artist_id = "artist_id", 
         album_title, 
         genre) %>%
  filter(genre == "Classique") %>% 
  as_tibble()

# --------------

# load composers
comp <- read.csv("data/comp_wide_1706.csv",
                 sep = ";")

comp <- comp %>% 
  as_tibble() %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) #%>% 
#select(dz_artist_id, dz_name) 

comp <- comp %>% 
  mutate(
    alias_norm = str_norm_klassik(aliases)
    )



## ------------ filter candidate albums (reduce computing time)

pat <- paste(comp$alias_norm, collapse = "|")
pat

classical_albums <- classical_albums %>%
  filter(str_detect(str_to_lower(album_title), pat))

## ------------ match composers with albums

classical_albums <- classical_albums %>%
  select(album_id, album_title) %>%
  crossing(comp) %>%
  filter(
    str_detect(
      str_to_lower(album_title),
      regex(
        paste0("\\b", dz_name, "\\b")
      )
    )
  ) %>% 
  distinct(album_id, dz_artist_id) %>%
  group_by(album_id) %>%
  summarise(
    composer_dz_artist_id = list(unique(dz_artist_id)),
    .groups = "drop"
  )












