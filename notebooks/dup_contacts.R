# --------------------- CONTACTS
tar_load(contacts)

contacts <- contacts %>% 
  #distinct(contact_id, mbz_id, .keep_all = T) %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(mbz_id, name = "n_mbz") %>% 
  select(contact_id, mbz_id, contact_name, n_co, n_mbz)

# name + mbz duplicates
t <- contacts %>% 
  add_count(contact_name) %>% 
  filter(!is.na(contact_name)) %>% 
  filter(n_mbz > 1) %>% 
  filter(!is.na(mbz_id)) %>% 
  filter(n > 1) %>% 
  arrange(desc(mbz_id))

# 26 duplicate mbz ids with differing names, and they all clearly map to the same artist
# so here it is fine to merge all contacts with duplicate mbz_ids
a <- co_dups_mbz_id %>% 
  anti_join(t, by = "mbz_id")


# contact_id dups: 0
# --> mbz_id is always unique
co_dups_contact_id <- contacts %>% 
  filter(n_co > 1)

# mbz_id dups: 686 (for 1458 contact_ids)
# extract
co_dups_mbz_id <- contacts %>% 
  filter(!is.na(mbz_id)) %>% 
  filter(n_mbz > 1) %>% 
  arrange(desc(mbz_id))

max(co_dups_mbz_id$n_mbz) # max 8 contact_ids for one mbz




### MERGE TO HIGHER CONTACT_ID

test <- co_dups_mbz_id %>% 
  group_by(mbz_id) %>% 
  summarise(contact_id = max(contact_id),
            contact_name = first(contact_name)) %>% 
  arrange(desc(mbz_id))


## must not summarise mbz_id == NA to one contact_id!
## leave out and re-join later?

contacts %>% 
  filter(is.na(mbz_id))


contacts
test <- contacts %>% 
  filter(!is.na(mbz_id)) %>% 
  group_by(mbz_id) %>% 
  summarise(contact_id = max(contact_id),
            contact_name = first(contact_name))


