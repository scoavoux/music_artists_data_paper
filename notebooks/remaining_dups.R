library(dplyr)
library(stringr)

## identify remaining duplicates



## separately for contacts and mbz

alldup <- all %>%
  #filter(!is.na(contact_id) | !is.na(musicbrainz_id)) %>% # needed
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)


codup <- alldup %>% 
  filter(collection_count > 0) %>% 
  filter(!is.na(contact_id)) %>% 
           filter(n_co > 1) %>% 
  group_by(contact_id) %>% 
  mutate(pop_sum = sum(pop),
         pop_share = pop / sum(pop)) %>% 
  arrange(desc(contact_id))
  
deezdup <- alldup %>% 
  filter(!is.na(contact_id)) %>% 
  filter(collection_count > 0) %>% 
  filter(n_deezer > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(col_sum = sum(collection_count),
         col_share = collection_count / sum(collection_count)) %>% 
  arrange(desc(deezer_id))

codup %>% 
  select(name, deezer_id, contact_id, pop_share)

all %>% 
  filter(str_detect(name, "Caballero")) %>% 
  arrange(desc(collection_count))




s# remaining deezer dups (collection_count > 0)

deezer_dups <- alldup %>% 
  filter(n_deezer > 1) %>% 
  filter(collection_count > 0) %>% 
  group_by(deezer_id) %>% 
  mutate(col_share = if_else(is.na(collection_count),
                             0, 
                             collection_count / sum(collection_count)))

library(tidyr)

canonical <- codup %>%
  group_by(contact_id) %>%
  mutate(
    cc = replace_na(collection_count, 0),
    pop = replace_na(pop, 0)
  ) %>%
  arrange(desc(cc), desc(pop)) %>%
  slice(1) %>%
  ungroup()


canonical %>%
  count(contact_id) %>%
  filter(n > 1)












