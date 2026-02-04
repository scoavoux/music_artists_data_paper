library(dplyr)
library(stringr)

## identify remaining duplicates separately for contacts and mbz
alldup <- all %>%
  filter(!is.na(contact_id) | !is.na(musicbrainz_id)) %>% # needed
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)


# artists with  > 1 deezer accounts
deezer_dup <- alldup %>% 
  #filter(!is.na(contact_id)) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(pop_sum = sum(pop),
         pop_share = pop / sum(pop)) %>% 
  select(name, contact_name, deezer_id, contact_id, pop, collection_count, pop_share)
arrange(desc(contact_id))





## first: deezer duplicates due to contacts
deezer_codup <- alldup %>% 
  filter(!is.na(contact_id)) %>% 
  filter(collection_count > 0) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(col_sum = sum(collection_count),
         col_share = collection_count / sum(collection_count)) %>% 
  arrange(desc(deezer_id))

## second: deezer duplicates due to musicbrainz
deezer_mbzdup <- alldup %>% 
  filter(!is.na(musicbrainz_id)) %>% 
  filter(collection_count > 0) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(col_sum = sum(collection_count),
         col_share = collection_count / sum(collection_count)) %>% 
  arrange(desc(deezer_id))
















