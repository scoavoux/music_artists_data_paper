library(dplyr)
library(stringr)

tar_load(all_enriched)


## identify remaining duplicates separately for contacts and mbz
alldup <- all_enriched %>%
  filter(!is.na(contact_id) | !is.na(musicbrainz_id)) %>% # needed
  filter(collection_count > 0) %>% 
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)


#  --------------------------------
# deezer duplicates collection_count > 0
deezer_dup <- alldup %>% 
  #filter(collection_count > 0) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  group_by(deezer_id) %>% 
  mutate(col_sum = sum(collection_count),
         col_share = collection_count / sum(collection_count)) %>% 
  select(name, contact_name, deezer_id, musicbrainz_id,
         contact_id, col_sum, col_share, collection_count, pop) %>% 
  arrange(desc(pop))

sum(deezer_dup$pop)



### !!!!!!!!!!!! --> for them, take the more popular one and keep their pop_share !!!!!!!!!!!!!


#  --------------------------------
## contact duplicates
contacts_dup <- alldup %>% 
  filter(!is.na(contact_id)) %>% 
  filter(n_co > 1) %>% 
  group_by(contact_id) %>% 
  mutate(max_pop = max(pop),
         pop_sum = sum(pop),
         pop_share = pop / sum(pop)) %>% 
  arrange(desc(max_pop))



#  --------------------------------
## deezer duplicates due to musicbrainz
deezer_mbzdup <- alldup %>% 
  filter(!is.na(musicbrainz_id)) %>% 
  filter(collection_count > 0) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(col_sum = sum(collection_count),
         col_share = collection_count / sum(collection_count)) %>% 
  arrange(desc(deezer_id))
















