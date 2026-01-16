# enrich the consolidated artists file "all"
# with mbz ids from wiki
# and with mbz ids + contact_ids through unique name matches


library(dplyr)


tar_load(all)

tar_load(contacts)

contacts <- contacts %>% 
  as_tibble %>% 
  mutate_if(is.integer, as.character)

cleanpop(all)
## ---------------------------- ADD MBZ by wiki_id
wiki_mbz <- wiki %>% 
  select(deezer_id = deezerID, musicBrainzID) %>% 
  filter(!is.na(musicBrainzID)) %>% 
  distinct(deezer_id, .keep_all = TRUE)

all <- left_join_coalesce(
  all,
  wiki_mbz,
  by = "deezer_id",
  col = "musicBrainzID"
)

cleanpop(all)


## -------- ENRICH WITH unique names from CONTACTS
contacts_ref <- contacts %>% 
  select(contact_id, contact_name) %>% 
  filter(!is.na(contact_name)) %>% # only contacts with names
  anti_join(all, by = "contact_id") # only contacts not already in all

added_contacts <- unique_name_match(
  miss = all %>% filter(is.na(contact_id)),
  ref = contacts_ref,
  miss_name = "name",
  ref_name = "contact_name",
  id_col = "contact_id"
)

all <- left_join_coalesce(
  all,
  added_contacts,
  by = "deezer_id",
  col = "contact_id"
)

cleanpop(all)

all %>% 
  filter(name == "13 Block")


## -------- ENRICH WITH MBZ NAMES FROM ...MBZ
## WOW!! adds a LOT of cases

mbz_ref <- mbz_deezer %>% 
  select(musicBrainzID, mbz_name) %>% 
  filter(!is.na(mbz_name)) %>% 
  anti_join(all, by = "musicBrainzID")

added_mbz <- unique_name_match(
  miss = all %>% filter(is.na(musicBrainzID)),
  ref = mbz_ref,
  miss_name = "name",
  ref_name = "mbz_name",
  id_col = "musicBrainzID"
)

all %>% 
  filter(name == "YUZMV")


cleanpop(all)

all <- left_join_coalesce(
  all,
  added_mbz,
  by = "deezer_id",
  col = "musicBrainzID"
)

cleanpop(all)



## -------- ENRICH WITH MBZ NAMES FROM WIKI
wiki_ref <- wiki %>% 
  select(musicBrainzID, mbz_name) %>% 
  filter(!is.na(mbz_name)) %>% # only mbz with names
  anti_join(all, by = "musicBrainzID") # only mbz not already in all

added_mbz <- unique_name_match(
  miss = all %>% filter(is.na(musicBrainzID)),
  ref = wiki_ref,
  miss_name = "name",
  ref_name = "mbz_name",
  id_col = "musicBrainzID"
)


all <- left_join_coalesce(
  all,
  added_mbz,
  by = "deezer_id",
  col = "musicBrainzID"
)

cleanpop(all) # 84.5% of streams covered after operations


### CHECK DIFFERENT DUPLICATES
### metrics for deezer-mbz, deezer_contacts, contacts-mbz, deezer-mbz-contacts

nrow(all) - nrow(all %>% 
                   distinct(deezer_id))

nrow(all) - nrow(all %>% 
                   distinct(deezer_id, musicBrainzID))

nrow(all) - nrow(all %>% 
                   distinct(deezer_id, contact_id))

nrow(all) - nrow(all %>% 
                   distinct(deezer_id, musicBrainzID, contact_id))

nrow(all %>% 
       filter(!is.na(musicBrainzID))) - nrow(all %>% 
                                               filter(!is.na(musicBrainzID)) %>% 
                                               distinct(musicBrainzID, contact_id))

# export consolidated artists
write_s3(all, "interim/consolidated_artists.csv")
































