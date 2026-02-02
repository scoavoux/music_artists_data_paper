## identify remaining deezer duplicates -- not many!

alldup <- all_enriched %>%
  filter(!is.na(contact_id) & !is.na(musicbrainz_id)) %>% # needed
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)

t <- alldup %>% 
  distinct(deezer_id, .keep_all = T)

sum(t$pop) # remaining stream share



deezer_dups <- alldup %>% 
  filter(n_co == 1 & n_mbz == 1) %>% 
  arrange(desc(pop))

deezer_dups <- deezer_dups %>% 
  filter(collection_count > 0) %>% 
  group_by(deezer_id) %>% 
  mutate(col_share = if_else(is.na(collection_count),
                             0, 
                             collection_count / sum(collection_count)))


## do the same for mbz and contacts

co_dups <- alldup %>% 
  filter(n_co > 1) %>% 
  arrange(desc(contact_id))

mbz_dups <- alldup %>% 
  filter(n_mbz > 1) %>% 
  arrange(desc(musicbrainz_id))

deezer_dups <- deezer_dups %>% 
  filter(collection_count > 0) %>% 
  group_by(deezer_id) %>% 
  mutate(col_share = if_else(is.na(collection_count),
                             0, 
                             collection_count / sum(collection_count)))








write.csv2(dups, "data/remaining_dups.csv")







