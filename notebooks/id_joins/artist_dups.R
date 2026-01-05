# possible rule: compare completeness of duplicates,
# i.e. if one duplicate has all ids and the other
# only has mbz and itemId, take the first one

test <- wiki_ids %>% 
  full_join(mbz_deezer, by = "musicBrainzID") %>% 
  mutate(deezerID = coalesce(deezerID.x, deezerID.y)) %>% 
  

dup <- test %>% 
  group_by(deezerID) %>% 
  summarize(n = n()) %>% 
  filter(n > 1)

dup <- test %>%
  group_by(itemId) %>%
  mutate(n_wiki = n()) %>%
  filter(n_wiki > 1 & !is.na(itemId)) %>% 
  arrange(desc(n_wiki)) %>% 
  left_join(artists, by = c(deezerID = "deezer_id")) %>% 
  filter(!is.na(f_n_play))



# 1466 duplicates, from 2 (almost always) to 17 instances of deezer_id
# we can merge those mbz duplicates with fairly easy rules
# check name similarity, genre, maybe releases...
valid_mbz_dup <- valid_mbz %>% 
  group_by(deezer_id, name, f_n_play) %>% 
  summarise(n = n()) %>% 
  filter(n > 1) %>% 
  arrange(desc(n))