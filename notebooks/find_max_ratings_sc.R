tar_load(senscritique)
tar_load(sc_ratings)

senscritique

senscritique <- senscritique %>% 
  left_join(sc_ratings, by = "sc_artist_id")

rm_dups <- load_s3("interim/removed_duplicates.csv")

rm_dups <- rm_dups %>% 
  mutate_if(is.integer, as.character)


t <- senscritique %>% 
  anti_join(all_dedup, by = "sc_artist_id") %>% 
  anti_join(rm_dups, by = "sc_artist_id") %>% 
  filter(!is.na(n_ratings)) %>% 
  arrange(desc(n_ratings))

## anti_join with filtered dups

library(stringr)

all_dedup %>% 
  filter(is.na(sc_artist_id)) %>% 
  filter(str_detect(dz_name, "Miles Davis")) %>% 
  select(dz_name, dz_artist_id, pop)

















