
pairings0 <- read.csv("data/manual_search.csv")

# matchs uniques entre api deezer et sc (sur le nom)
pairings1 <- load_s3(file = "senscritique/senscritique_id_deezer_id_pairing.csv")

## -----------------------------

pairings2 <- load_s3(file = "senscritique/senscritique_deezer_id_pairing_2.csv")
pairings3 <- load_s3(file = "senscritique/senscritique_deezer_id_pairing_3.csv")
pairings4 <- load_s3(file = "senscritique/senscritique_deezer_id_pairing_4.csv")


#### Put them together ------
pairings <- pairings1 %>% 
  select(contact_id, 
         deezer_id = "artist_id") %>% 
  bind_rows(pairings0,
            select(pairings2, contact_id, artist_id = "deezer_id"),
            pairings3,
            pairings4) %>% 
  distinct()

pairings <- pairings %>% 
  filter(!is.na(artist_id))

pairings <- tibble(pairings) %>% 
  select(contact_id, deezer_id)

write.csv(pairings, "data/pairings.csv")





