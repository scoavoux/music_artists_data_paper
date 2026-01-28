library(sjmisc)

tar_load(all_enriched)

all_count <- all_enriched %>% 
  filter(!is.na(musicbrainz_id) | !is.na(contact_id)) %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(contact_id, name = "n_co")

all_dups <- all_count %>% 
  filter(n_deezer > 1 |
           n_mbz > 1 |
           n_co > 1)


###### for co + mbz: TEST pop_share for disambiguation!
co_dups <- all_dups %>% 
  filter(n_co > 1) %>%
  group_by(contact_id) %>%
  mutate(max_pop = max(pop, na.rm = TRUE),
         pop_share = pop / sum(pop)) %>%
  arrange(desc(max_pop), desc(pop)) %>%
  ungroup() %>% 
  #filter(!is.na(contact_id)) %>% 
  select(name, contact_name, deezer_id, contact_id, pop, pop_share)

co_dups %>%
  filter(pop_share > 0.9)

mbz_dups <- all_dups %>% 
  filter(n_mbz > 1) %>%
  group_by(musicbrainz_id) %>%
  mutate(max_pop = max(pop, na.rm = TRUE),
         pop_share = pop / sum(pop)) %>%
  arrange(desc(max_pop), desc(pop)) %>%
  ungroup() %>% 
  #filter(!is.na(contact_id)) %>% 
  select(name, mbz_name, deezer_id, musicbrainz_id, pop, pop_share)

mbz_dups %>%
  filter(pop_share < 0.9 & pop_share > 0.01 & pop_share != 0.5)



deezerdups %>% 
  distinct(contact_id, musicbrainz_id, deezer_id)

deezerdups <- deezerdups %>%
  left_join(contacts %>% 
              select(contact_id, collection_count),
            by = "contact_id")

t <- deezerdups %>% 
  filter(collection_count > 0) %>% # remove irrelevant artists 
  group_by(deezer_id) %>% 
  mutate(collection_count = as.integer(collection_count)) %>% 
  mutate(col_share = collection_count / sum(collection_count)) #%>% 
  #filter(col_share > 0.9)





### TO WIDE (for export??)
for_wide <- all_dups %>% 
  filter(n_co > 5) # drop extreme cases

df_wide <- all_dups %>%
  group_by(deezer_id) %>%
  mutate(dup_n = row_number()) %>%
  pivot_wider(
    id_cols = c(deezer_id, name),
    names_from = dup_n,
    values_from = c(contact_id, musicbrainz_id),
    names_glue = "{.value}_{dup_n}"
  )

