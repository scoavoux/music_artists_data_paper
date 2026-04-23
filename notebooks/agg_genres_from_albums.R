
albums <- load_s3("records_w3/genres_from_albums.parquet")
  
threshold <- 0.3

albums <- albums %>% 
  left_join(deezer_genre_mapping, by = "genre_id") %>% 
  filter(!is.na(genre)) 


# table(albums$record_type, useNA = "always")

artist_genre <- albums %>%
  #filter(record_type == "album") %>% 
  group_by(artist_id, genre) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(artist_id) %>%
  mutate(prop = n / sum(n)) %>%
  filter(prop >= threshold) %>%
  arrange(artist_id, desc(prop)) %>%
  slice(1) %>%  # keep first genre
  ungroup() %>% 
  select(dz_artist_id = "artist_id",
         genre)

t <- df %>% 
  left_join(artist_genre, by = "dz_artist_id") %>% 
  select(dz_name, n_plays_share, genre, genre_dz_main)

prop_na(t)



## aller chercher les genres individuellement DONE (in map_deezer_genres.R)
### on en récupère 25 environ, dont des genres orphelins

## aller voir si -single enlève des artistes DONE
### avec albums uniquement on passe de 239k à 177k artistes
### on perd 4.8% de streams avec un risque de virer de nouveaux artistes



### essayer pondération par nb fans (log?)

### essayer premier(s) albums de chaque artiste

### comparer tout ça --> corrélation


### pour chaque classification regarder le top 50 dans chaque genre
### regarder les artistes où ça diffère







