
genre <- load_s3("records_w3/genres_from_albums.parquet")
deezer_genre_mapping <- load_s3("records_w3/deezer_genre_mapping.csv")

genre <- genre %>%
  left_join(deezer_genre_mapping, by = "genre_id") %>%
  filter(!is.na(genre)) %>%
  select(album_id, dz_artist_id = "artist_id", album_title, genre) %>%
  as_tibble()

genre_klassik <- genre %>%
  filter(genre == "Classique")


dz_songs


t %>% 
  distinct(dz_artist_id, .keep_all = T)

table(albums$main_genre)



### OR: classical artists over mbz (from df???) --> 

cl <- df %>% 
  filter(genre_mbz_album_1 == "classical" |
           genre_mbz_artist_1 == "classical" |
           genre_dz_album_1 == "Classique" | # ALMOST ALL ARTISTS FROM HERE
           genre_dz_artist == "Classique")


t <- albums_klassik %>% 
  inner_join(cl, by = "dz_artist_id")



genre_klassik %>% 
  filter(str_detect(album_title, "Berlioz"))













