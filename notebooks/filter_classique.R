
library(stringi)



helene <- read.csv2("data/ignoredata/base_compositeurs.csv")

helene <- helene %>% 
  filter(films == 0 & chef.d.orch == 0 & jeux.vidéo == 0 & interprète == 1) %>% 
  as_tibble() %>% 
  distinct(dz_artist_id, .keep_all = T)


# old df without correction
raw_df <- read.csv2("data/ignoredata/df.csv")

raw <- raw_df %>% 
  filter(genre_dz_album_1 == "Classique") %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  select(dz_name, dz_artist_id, n_plays, n_plays_raw) %>% 
  as_tibble()

treated <- df %>% 
  filter(genre_dz_album_1 == "Classique") %>% 
  select(dz_name, dz_artist_id, n_plays, n_plays_raw)

treated %>% 
  filter(str_detect(dz_name, "Mozart"))

options(scipen = 99)


t <- raw %>% 
  inner_join(treated, by = "dz_artist_id") %>% 
  mutate(diff_raw = as.integer(n_plays_raw.y - n_plays_raw.x),
         factor_raw = diff_raw / n_plays_raw.x,
         diff = as.integer(n_plays.y - n_plays.x),
         factor = diff / n_plays.x) %>% 
  
  arrange(desc(factor_raw)) %>% 
  select(dz_name.x, diff_raw, factor_raw, diff, factor)

test <- t %>% 
  filter(diff != 0 & abs(diff) > 5) %>% 
  arrange(factor_raw)

write.csv2(test, "data/composers_n_plays_diff.csv")  


# ----------------------------------------------------------

albums <- load_s3("interim/prod/genres_from_albums.parquet")
deezer_genre_mapping <- load_s3("interim/dict/deezer_genre_mapping.csv")

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

# load base compositeurs
comp <- read.csv("data/composers.csv",
                 sep = ";")

comp <- comp %>% 
  as_tibble() %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  select(dz_artist_id, dz_name) 

## ------------ filter candidate albums (reduce computing time)

pat <- paste(comp$dz_name, collapse = "|")

classical_albums <- classical_albums %>%
  filter(str_detect(str_to_lower(album_title), pat))

## ------------ match composers with albums


classical_albums <- classical_albums %>%
  select(album_id, album_title) %>%
  tidyr::crossing(comp) %>%
  filter(
    str_detect(
      str_to_lower(album_title),
      regex(
        paste0("\\b", dz_name, "\\b")
      )
    )
  ) %>% 
  distinct(album_id, dz_artist_id) %>%
  group_by(album_id, album_title, dz_artist_id) %>%
  summarise(
    composer_dz_artist_id = list(unique(dz_artist_id)),
    .groups = "drop"
  )

pop <- df %>% 
  select(dz_artist_id, dz_name)

alb <- albums %>% 
  left_join(pop, by = c(artist_id = "dz_artist_id"))

alb %>% 
  filter(str_detect(dz_name, "Manoury"))





