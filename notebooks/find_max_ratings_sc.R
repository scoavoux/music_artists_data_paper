## find max sc-rated artists to check if we can retrieve them in deezer
## not very promising, most of them can't be found in deezer

tar_load(sc_ratings)


contacts <- load_s3("senscritique/contacts.csv")

contacts <- contacts %>% 
  filter(subtype_id == 13) %>% 
  as_tibble() %>% 
  mutate(sc_artist_id = as.character(contact_id)) %>% 
  select(sc_artist_id, contact_name) %>% 
  left_join(sc_ratings, by = "sc_artist_id") %>% 
  as_tibble()


rm_dups <- load_s3("interim/removed_duplicates.csv")

rm_dups <- rm_dups %>% 
  mutate_if(is.integer, as.character)

t <- contacts %>% 
  anti_join(all_dedup, by = "sc_artist_id") %>% 
  anti_join(rm_dups, by = "sc_artist_id") %>% 
  filter(!is.na(n_ratings)) %>% 
  arrange(desc(n_ratings))


library(stringr)

all_dedup %>% 
  filter(is.na(sc_artist_id)) %>% 
  filter(str_detect(dz_name, "Miles Davis")) %>% 
  select(dz_name, dz_artist_id, pop)

















