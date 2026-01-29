# CONTACTS! (i.e. different deezer_ids)

library(sjmisc)

tar_load(all_enriched)

## work separately for co and mbz: subset !is.na(co)
## then !is.na(mbz)
## then solve deezer through collection count!

col_count <- contacts %>% 
  select(contact_id, collection_count)

co_dup <- all_enriched %>% 
  filter(!is.na(contact_id)) %>% 
  left_join(col_count, by = "contact_id") %>% 
  #add_count(deezer_id, name = "n_deezer") %>% 
  #add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(contact_id, name = "n_co") %>% 
  filter(n_co > 1) %>%
  # create pop share disambiguation col
  group_by(contact_id) %>%
  mutate(max_pop = max(pop, na.rm = TRUE),
         pop_share = pop / sum(pop)) %>%
  arrange(desc(max_pop), desc(pop))



###### use pop_share to disambiguate!
co_dups_to_keep <- co_dup %>% 
  group_by(contact_id) %>%
  mutate(max_pop = max(pop, na.rm = TRUE),
         pop_share = pop / sum(pop)) %>%
  arrange(desc(max_pop), desc(pop)) %>%
  ungroup() %>% 
  filter(!is.na(contact_id)) %>% 
  select(name, contact_name, deezer_id, 
         contact_id, pop, pop_share, n_co)

t <- co_dups %>% 
  filter(pop_share > 0.9)




