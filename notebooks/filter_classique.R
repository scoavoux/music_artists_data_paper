
library(stringi)


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
  mutate(diff = as.integer(n_plays.y - n_plays.x),
         factor = diff / n_plays.x) %>% 
  arrange(desc(factor)) %>% 
  select(dz_name.x, diff, factor)

test <- t %>% 
  filter(diff != 0 & abs(diff) > 5)

library(ggplot2)
ggplot2::ggplot(t, aes(x = factor)) +
  geom_histogram() +
  xlim(c(-0.5, 0.5))

write.csv2(test, "data/composers_n_plays_diff.csv")  











