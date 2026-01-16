
# --------------------- CONTACTS
tar_load(contacts)

contacts <- contacts %>% 
  as_tibble() %>%
  distinct(contact_id, mbz_id, .keep_all = T) %>% 
  mutate_if(is.integer, as.character) %>% 
  mutate(mbz_id = ifelse(mbz_id == "", NA, mbz_id)) %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(mbz_id, name = "n_mbz") %>% 
  select(contact_id, mbz_id, contact_name, n_co, n_mbz)


# no contact_id dups!
# meaning that mbz_id is always unique
co_dups_contact_id <- contacts %>% 
  filter(n_co > 1)

# mbz_dups
co_dups_mbz_id <- contacts %>% 
  filter(!is.na(mbz_id)) %>% 
  filter(n_mbz > 1)


# --------------------- MANUAL SEARCH
manual_search %>% 
  filter(contact_id == 53)

manual_search <- manual_search %>% 
  rename(deezer_id = "artist_id") %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  as_tibble()

manual_search


# ----------------------- MBZ_DEEZER

mbz_deezer <- mbz_deezer %>% 
  distinct(deezerID, musicBrainzID, .keep_all = T) %>% 
  add_count(musicBrainzID, name = "n_mbz") %>% 
  add_count(deezerID, name = "n_deezer")

# multiple mbz_ids for one deezer_id
mbz_dups_deezer_id <- mbz_deezer %>% 
  filter(!is.na(deezerID)) %>% 
  filter(n_deezer > 1) %>% 
  arrange(desc(deezerID))

# multiple deezerIDs for one mbz_id
mbz_dups_mbz_id <- mbz_deezer %>% 
  filter(!is.na(musicBrainzID)) %>% 
  filter(n_mbz > 1) %>% 
  arrange(desc(musicBrainzID))



t <- artists %>% 
  inner_join(mbz_dups_mbz_id, by = c(deezer_id = "deezerID")) %>% 
  distinct(deezer_id, .keep_all = T)

t <- artists %>% 
  inner_join(mbz_dups_deezer_id, by = c(deezer_id = "deezerID")) %>% 
  distinct(deezer_id, .keep_all = T)

t %>% 
  arrange(desc(pop))













