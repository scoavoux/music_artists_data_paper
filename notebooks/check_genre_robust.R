library(sjmisc)
library(stringr)

tar_load(df)

# release languages
# check overlap with deezer main_lang
mbz_lang <- load_s3("musicbrainz/musicbrainz_artist_language_album_counts.csv")

# cluster genres (by co-occurrence) to classify them into
# the deezer album genres
mbz_genre <- load_s3("musicbrainz/musicbrainz_artist_genre_count.csv")

dz_name <- df %>% 
  select(dz_name, mbz_artist_id, n_plays_share)

mbz_genre <- mbz_genre %>% 
  as_tibble() %>% 
  rename(mbz_artist_id = "mbid",
         mbz_genre = genre_name) %>% 
  inner_join(dz_name, by = "mbz_artist_id") %>% 
  arrange(desc(n_plays_share))

# ------------------------------
mbz_genre_top <- mbz_genre %>% 
  add_count(mbz_genre) %>% 
  filter(n > 2000)

frq(mbz_genre_top$mbz_genre, sort.frq = "desc")
# ------------------------------

dat <- df %>% 
  select(dz_name, dz_artist_id, mbz_artist_id,
         genre_1, genre_2, genre_dz_main) %>% 
  left_join(mbz_genre_top, by = "mbz_artist_id") %>% 
  mutate(genre_1 = str_to_lower(genre_1),
         genre_2 = str_to_lower(genre_2))


# genre tables
mbz_genre_frq <- frq(mbz_genre$mbz_genre, sort.frq = "desc")
dz_genre_frq <- frq(df$genre_1, sort.frq = "desc")


# 23 valid deezer genres
pop, 
electro, 
rap, 
alternative, 
classique, 
rock, 
dance,
jazz, 
latino, 
r&b, 
films/jeux vidéo, 
reggae, 
folk,
country, 
metal, 
chanson française, 
soul & funk, 
blues

musique africaine, 
musique brésilienne,  
musique asiatique,
musique arabe,
musique indienne

genre_rename <- c(electronic = electro, # dance??
                  classical = classique,
                  hip hop = rap/hip Hop,
                  alternative rock = alternative,
                  latin = latino,
                  
                  )

main_genres <- c("rock", 
                 "electronic", # include downtempo 
                 "pop", 
                 "jazz", 
                 "classical", 

                 "hip hop", 
                 "punk", 
                 "folk",
                 "metal", 
                 "blues", 

                 "electro",
                 "alternative rock", 
                 "downtempo", 
                 "techno", 
                 "latin",
                 
                 "experimental", # include industrial
                 "noise", 
                 "ambient")




map_to_main <- function(genre, main_genres) {
  matches <- main_genres[str_detect(
    genre,
    paste0("\\b", main_genres, "\\b")
  )]
  
  if (length(matches) == 0) {
    return(NA_character_)
  } else {
    return(matches[1])
  }
}

mbz_genres <- unique(mbz_genre$mbz_genre)

# test with 300 genres first
genre_map <- mbz_genre_300 %>%
  distinct(mbz_genre) %>%
  mutate(
    genre_clean = mbz_genre %>%
      str_to_lower() %>%
      str_replace_all("-", " ") %>%
      str_replace_all("&", "and"),
    
    main_genre = map_chr(genre_clean, map_to_main, main_genres)
  )


genre_map %>% 
  filter(str_detect(genre_clean, "pop"))



t <- df %>% 
  inner_join(mbz_genre, by = "mbz_artist_id") %>% 
  select(dz_name, mbz_artist_id, mbz_genre, n_albums)

t %>% 
  distinct(mbz_artist_id)


genre_map <- genre_map %>% 
  arrange(desc(main_genre))


t <- mbz_genre %>% 
  filter(mbz_genre %in% c("electronic", "electro")) %>% 
  arrange(desc(n_plays_share))























