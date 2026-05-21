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
  left_join(mbz_genre, by = "mbz_artist_id") %>% 
  mutate(genre_1 = str_to_lower(genre_1),
         genre_2 = str_to_lower(genre_2))


# genre tables
mbz_genre_frq <- frq(mbz_genre$mbz_genre, sort.frq = "desc")[[1]]
dz_genre_frq <- frq(df$genre_1, sort.frq = "desc")[[1]]

write.csv2(mbz_genre_frq, "data/mbz_genre_frq.csv", sep = ";")



# RECODE TABLE
rec <- load_s3("data/mbz_genre_frq.csv", sep = ";")

rec <- rec %>% 
  as_tibble() %>% 
  filter(recode_genre != "")

mbz_genre <- mbz_genre %>% 
  left_join(rec, by = "mbz_genre")

## now aggregate: biggest genre by artist
## and look at NAs + correlation with deezer

























