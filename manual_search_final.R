tar_load(manual_search)

mbz <- read.csv("data/missing_mbz_1000.csv")

mbz <- mbz %>% 
  as_tibble() %>% 
  filter(!is.na(mbz_artist_id) | !is.na(sc_artist_id)) %>% 
  filter(artiste.à.supprimer.de.la.base != "x") %>% 
  mutate(dz_artist_id = as.character(dz_artist_id),
         sc_artist_id = as.character(sc_artist_id)) %>% 
  select(dz_artist_id, mbz_artist_id, sc_artist_id)


t <- bind_rows(manual_search, mbz) %>% 
  distinct()

write_s3(t, "interim/dict/manual_search_ids.csv")

