
## export biggest missings to csv for handcoding

tar_load(all_dedup)

library(stringr)

missing <- all_dedup %>%
  filter(is.na(contact_id) | is.na(musicbrainz_id)) %>%
  slice(1:1000)

missing_mbz <- all_dedup %>%
  filter(is.na(musicbrainz_id)) %>%
  slice(1:1000)

missing_contacts <- all_dedup %>%
  filter(is.na(contact_id)) %>%
  slice(1:1000)

cleanpop(missing)
cleanpop(missing_mbz)
cleanpop(missing_contacts)


write.csv2(missing_contacts, "data/missing_contacts.csv")

write_s3(missing, "interim/missings_to_handcode/missing_either.csv")
write_s3(missing_mbz, "interim/missings_to_handcode/missing_mbz.csv")
write_s3(missing_contacts, "interim/missings_to_handcode/missing_contacts.csv")




tar_load(contacts)



tar_load(manual_search)
manual_search

### import 270 hand-coded contacts 06.02

manual_co_1 <- load_s3("interim/missings_to_handcode/06.02-handcoded_contacts.csv")

manual_co_1 <- manual_co_1 %>% 
  as_tibble() %>% 
  filter(!is.na(contact_id)) %>% 
  mutate_if(is.integer, as.character) %>% 
  select(deezer_id, contact_id)

manual_search <- manual_search %>% 
  bind_rows(manual_co_1)



test <- manual_co_1 %>% 
  left_join(contacts, by = "contact_id")

test <- test %>% 
  filter(!is.na(contact_name))


test <- test %>% 
  inner_join(all_dedup, by = "deezer_id")

test <- test %>% 
  select(deezer_id, pop)








  
  
  
  
  
  