
albums <- load_s3("records_w3/genres_from_albums.parquet")
  
albums <- albums %>% 
  left_join(deezer_genre_mapping, by = "genre_id") %>% 
  filter(!is.na(genre)) %>% 
  rename(dz_artist_id = "artist_id")



### essayer pondération par nb fans (log?)

### essayer premier(s) albums de chaque artiste

### comparer tout ça --> corrélation

#### les artistes ont 2.79 genres en moyenne, certains en ont plus de 5
#### ce qui parlerait pour la métrique du nombre imo


### pour chaque classification regarder le top 50 dans chaque genre
### regarder les artistes où ça diffère


## to define genre: min n releases?

## weight albums (way?) more than singles!
## --> some artists have a lot of singles, but eg harry styles only has 7 releases
## --> weight by album/single and fans

## assign genre 1 and genre 2? --> eg folk and rock for bob dylan















