library(dplyr)

## IDENTIFY DUPLICATES LEADING TO DIFFERENT ARTISTS!!

## --> maybe we shouldn't care about duplicate ids so much
## and focus on duplicate ids who point to different artists
## meaning: look at name differences too!

## also: maybe create a column for duplicates to flag
## original duplicates (before the joins)?


## LEAVE THIS FOR NOW, IGNORE THE ISSUE AND COME BACK LATER

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


# --------------------- MANUAL SEARCH
tar_load(manual_search)

manual_search <- manual_search %>% 
  rename(deezer_id = "artist_id") %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  as_tibble()

# 552 contact_ids for 1511 deezer ids
man_dups_contact_id <- manual_search %>% 
  filter(n_co > 1) %>% 
  arrange(desc(contact_id))

#
man_dups_deezer_id <- manual_search %>% 
  filter(n_deezer > 1) %>% 
  arrange(desc(deezer_id))

max(man_dups_contact_id$n_deezer)





# ----------------------- MBZ_DEEZER
tar_load(mbz_deezer)

mbz_deezer <- mbz_deezer %>% 
  distinct(deezer_id, musicbrainz_id, .keep_all = T) %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(deezer_id, name = "n_deezer")

# multiple mbz_ids for one deezer_id
mbz_dups_deezer_id <- mbz_deezer %>% 
  filter(!is.na(deezer_id)) %>% 
  filter(n_deezer > 1) %>% 
  arrange(desc(deezer_id))

# multiple deezer_ids for one mbz_id
mbz_dups_mbz_id <- mbz_deezer %>% 
  filter(!is.na(musicbrainz_id)) %>% 
  filter(n_mbz > 1) %>% 
  arrange(desc(musicbrainz_id))


























