
## export biggest missings to csv for handcoding

tar_load(all_dedup)

library(stringr)

missing <- all_dedup %>%
  filter(is.na(contact_id) | is.na(musicbrainz_id)) %>%
  slice(1:1000) %>% 
  select(name, deezer_id, musicbrainz_id, pop, contact_id)

missing_mbz <- all_dedup %>%
  filter(is.na(musicbrainz_id)) %>%
  slice(1:1000) %>% 
  select(name, deezer_id, musicbrainz_id, pop, contact_id)

missing_contacts <- all_dedup %>%
  filter(is.na(contact_id)) %>%
  slice(1:1000) %>% 
  select(name, deezer_id, musicbrainz_id, pop, contact_id)

cleanpop(missing)
cleanpop(missing_mbz)
cleanpop(missing_contacts)


write.csv2(missing_contacts, "data/missing_contacts.csv")
write.csv2(missing_mbz, "data/missing_mbz.csv")



### -------------- import hand-coded cases ---------------

### senscritique

manual_sc_1 <- load_s3("interim/missings_to_handcode/06.02-handcoded_contacts.csv")

manual_sc_2

manual_sc_1 <- manual_co_1 %>% 
  as_tibble() %>% 
  filter(!is.na(contact_id)) %>% 
  mutate_if(is.integer, as.character) %>% 
  select(deezer_id, contact_id)



manual_search <- manual_search %>% 
  bind_rows(manual_sc_1)


### mbz

manual_mbz_1




  
  
  
  
  
  