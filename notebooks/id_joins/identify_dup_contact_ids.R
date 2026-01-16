

tar_load(contacts)

contacts <- contacts %>% 
  as_tibble() %>% 
  mutate_if(is.integer, as.character) %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(mbz_id, name = "n_mbz") %>% 
  select(contact_id, mbz_id, contact_name, n_co, n_mbz)

# no contact_id dups!
co_dups <- contacts %>% 
  filter(n_co > 1)

# 
mbz_dups <- contacts %>% 
  filter(n_mbz > 1)


t <- all %>% 
  filter(contact_id == 53)

# wtf?! so not only is there no duplicates in contact_id 53,
# contact_id 53 doesn't exist at all in contacts!!
contacts %>% 
  filter(contact_id == 53)

manual_search <- manual_search %>% 
  as_tibble %>% 
  mutate_if(is.integer, as.character) 

# LOL it comes from manual_search
manual_search %>% 
  filter(contact_id == 53)

manual_search <- manual_search %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  as_tibble()

manual_search %>% 
  distinct(contact_id, deezer_id, .keep_all = T)

manual_search %>% 
  filter(contact_id == 53) %>% 
  distinct(deezer_id)













