
## export biggest missings to csv for handcoding

tar_load(all_final)

library(stringr)


missing_mbz <- all_final %>%
  filter(is.na(mbz_artist_id)) %>%
  slice(1:1000) %>% 
  select(dz_name, mbz_artist_id, dz_artist_id, dz_stream_share, sc_artist_id)

missing_sc <- all_final %>%
  filter(is.na(sc_artist_id)) %>%
  slice(1:1000) %>% 
  select(dz_name, sc_artist_id, dz_artist_id, mbz_artist_id, dz_stream_share)

print_stream_share(missing_mbz)
print_stream_share(missing_sc)


write.csv2(missing_sc, "data/missing_sc_1000.csv")
write.csv2(missing_mbz, "data/missing_mbz_1000.csv")


92.5/100

99.5/100



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



  
  
  
  
  
  