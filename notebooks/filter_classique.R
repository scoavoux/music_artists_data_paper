
library(stringi)

pop <- df %>% 
  select(dz_artist_id, n_plays_share)

# very famous composers
tar_load(aliases_to_add)


comp <- read.csv("data/base_compositeurs.csv",
                 sep = ";")

comp <- comp %>% 
  mutate(dz_artist_id = as.character(dz_artist_id),
         dz_name = str_normalize(dz_name)) %>% 
  filter(compositeur == 1) %>% 
  as_tibble() %>%
  left_join(pop, by = "dz_artist_id") %>% 
  arrange(desc(n_plays_share)) %>% 
  distinct(dz_name, .keep_all = T) %>% 
  select(dz_artist_id, dz_name, n_plays_share) 

aliases <- comp %>% 
  inner_join(aliases_to_add, by = "dz_name") %>% 
  distinct(dz_artist_id, .keep_all = T) %>% 
  select(dz_artist_id, dz_name = "ent_name")

comp <- comp %>% 
  bind_rows(aliases) %>% 
  distinct(dz_name, .keep_all = T)


# old df without correction
raw_df <- read.csv2("data/df.csv")

raw <- raw_df %>% 
  filter(genre_dz_album_1 == "Classique") %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  select(dz_name, dz_artist_id, n_plays, n_plays_raw) %>% 
  as_tibble()

tar_load(df)
treated <- df %>% 
  filter(genre_dz_album_1 == "Classique") %>% 
  select(dz_name, dz_artist_id, n_plays, n_plays_raw)

raw %>% 
  filter(str_detect(dz_name, "Mozart"))

options(scipen = 99)


t <- raw %>% 
  inner_join(treated, by = "dz_artist_id") %>% 
  mutate(diff = as.integer(n_plays.y - n_plays.x),
         factor = diff / n_plays.x) %>% 
  arrange(desc(factor)) %>% 
  select(dz_name.x, diff, factor)

composer_dict %>% 
  filter(str_detect(dz_name, "bach"))













