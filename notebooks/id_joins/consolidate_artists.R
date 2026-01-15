# Load all "raw" id files and bind them to one dataset
# enrich with mbz ids from wiki,
# and with mbz ids + contact_ids through unique name matches
library(dplyr)

# RERUN WITH NEW ARTISTS
# SWITCH DISTINCTS TO RAW FILES



# LOAD RAW -------------------------------------------

# artists in items
tar_load(artists)

artists <- artists %>%
  rename(deezer_id = "deezer_feat_id")

# musicbrainz keys
mbz_deezer <- load_s3("interim/musicbrainz_urls_collapsed_new.csv") # !! NEW FILE !!

mbz_deezer <- tibble(mbz_deezer) %>% 
  filter(!is.na(deezerID)) %>% 
  distinct(deezerID, musicBrainzID, mbz_name) # need to distinct bc dropping spotify etc leaves ~22k duplicates

# contacts keys
contacts <- load_s3("senscritique/contacts.csv")
contacts <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name, mbz_id, spotify_id)

# sam's manual searches
manual_search <- read.csv("data/manual_search.csv")
manual_search <- manual_search %>% 
  as_tibble() %>% 
  mutate(deezer_id = as.character(artist_id)) %>% 
  select(-artist_id) %>% 
  distinct(contact_id, deezer_id) # drop 66 perfect duplicates

## check uniqueness of cases
## after correcting manual_search, all raw data are unique
nrow(artists) - nrow(artists %>% distinct(deezer_id))
nrow(mbz_deezer) - nrow(mbz_deezer %>% distinct(musicBrainzID, deezerID))
nrow(contacts) - nrow(contacts %>% distinct(contact_id, mbz_id))
nrow(manual_search) - nrow(manual_search %>% distinct(contact_id, deezer_id)) # 66 duplicates


# CREATE ALL
all <- artists %>% 
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  left_join(contacts,  by = c(musicBrainzID = "mbz_id")) %>% 
  left_join(manual_search, by = "deezer_id") %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(name, contact_name, mbz_name, deezer_id, 
         musicBrainzID, contact_id, pop)

cleanpop(all) # covered streams


## ---------------------------- ADD WIKI-MBZ
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


## -------- ENRICH WITH CONTACTS
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





rstudioapi::writeRStudioPreference("pane_config", list(
  console = "right",
  source = "left",
  tabSet1 = "right",
  tabSet2 = "right",
  hiddenTabSet = "none"
))

rstudioapi::applyTheme("Merbivore Soft")
































