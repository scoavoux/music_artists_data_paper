library(dplyr)

## IDENTIFY DUPLICATES LEADING TO DIFFERENT ARTISTS!!

## --> maybe we shouldn't care about duplicate ids so much
## and focus on duplicate ids who point to different artists
## meaning: look at name differences too!

## also: maybe create a column for duplicates to flag
## original duplicates (before the joins)?




# -------------------- ARTISTS


t <- items %>% 
  group_by(deezer_feat_id) %>% 
  summarise(name = first(name))
  add_count(name) %>% 
  filter(name > 1) %>% 
  arrange(desc(name))

t <- artists %>% 
  add_count(name) %>% 
  filter(n > 1) %>% 
  arrange(desc(name))



all_enriched %>% 
  filter(str_detect(name, "Bob Marley"))
  
# wiki and/or deezer could help solve these cases
t <- all_enriched %>% 
  add_count(deezer_id) %>% 
  filter(n > 1)










# --------------------- CONTACTS
tar_load(contacts)

contacts <- contacts %>% 
  as_tibble() %>%
  #distinct(contact_id, mbz_id, .keep_all = T) %>% 
  mutate_if(is.integer, as.character) %>% 
  mutate(mbz_id = ifelse(mbz_id == "", NA, mbz_id),
         contact_name = ifelse(contact_name == "", NA, contact_name)) %>% 
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


t <- artists %>% 
  filter(str_detect(name, "Joan Jett"))


full <- all_enriched %>% 
  filter(!is.na(contact_id)) %>% 
  filter(!is.na(musicbrainz_id))

t <- full %>% 
  add_count(contact_id) %>% 
  filter(n > 1) %>% 
  arrange(desc(contact_id))
t


















