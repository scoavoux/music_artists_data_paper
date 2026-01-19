library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)



# LOAD RAW -------------------------------------------


# artists in items
tar_load(artists)

# musicbrainz keys
mbz_deezer <- load_s3("interim/musicbrainz_urls_collapsed.csv")
mbz_deezer <- tibble(mbz_deezer) %>% 
  filter(!is.na(deezer)) %>% 
  rename(musicBrainzID = "musicbrainz_id",
         deezerID = "deezer") # %>% 
  #select(musicBrainzID, deezerID)

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
  select(-artist_id)


# my wiki table
wiki <- load_s3("interim/wiki_ids.csv")

# -----------------------------------------------------------
# JOIN ALL POSSIBLE ITEM ARTISTS TO MBZ AND CONTACTS

all <- artists %>% 
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  left_join(contacts, by = c(musicBrainzID = "mbz_id")) %>% 
  left_join(manual_search, by = "deezer_id") %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(name, 
         contact_name, 
         deezer_id, 
         musicBrainzID, 
         contact_id, 
         f_n_play) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, .keep_all = T)

nrow(all)


# --------------- JOIN MBZ FROM WIKI to the cases missing in "all"

# subset artists with missing mbz ids
mbz_missing <- all %>% 
  filter(is.na(musicBrainzID))

# inner_join of the missing mbz cases with wikidata's mbz
mbz_from_wiki <- mbz_missing %>% 
  inner_join(wiki, by = c(c(deezer_id = "deezerID"))) %>% 
  mutate(musicBrainzID = musicBrainzID.y) %>% 
  filter(!is.na(musicBrainzID)) %>% 
  distinct(deezer_id, musicBrainzID, .keep_all = T) %>% 
  select(deezer_id, name, musicBrainzID, f_n_play)

pop(mbz_from_wiki) # retrieving 1.8% of missing mbz ids --- immerhin


mbz_from_wiki <- mbz_from_wiki %>% 
  mutate(contact_id = NA) %>% 
  select(deezer_id, musicBrainzID)


# merge missing mbz ids back into "all" and save
all <- all %>% 
  left_join(mbz_from_wiki, by = "deezer_id") %>% 
  mutate(musicBrainzID = coalesce(musicBrainzID.x,
                                  musicBrainzID.y)) %>% 
  select(-c(musicBrainzID.x, musicBrainzID.y))


#### --------------- ADD MBZ NAMES

mbid_name <- load_s3("musicbrainz/mbid_name_alias.csv")

mbz_name <- mbid_name %>% 
  filter(type == "name") %>% 
  as_tibble() %>% 
  select(musicBrainzID = "mbid",
         mbz_name = "name")

all <- all %>% 
  left_join(mbz_name, by = "musicBrainzID")



# -------------------- ADD SC OBTAINED FROM UNIQUE PERFECT MATCHES

# subset missing contact ids
miss <- all %>% 
  filter(is.na(contact_id)) %>% 
  select(-contact_id)

# contacts
contacts <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name) %>% 
  anti_join(all, by = "contact_id") # exclude found contact_ids


### overview of occurrences
### DESC ONLY
matches <- miss %>% 
  inner_join(contacts, by = c(name = "contact_name")) %>% 
  group_by(name) %>% 
  count() %>% 
  arrange(desc(n))

#### inner join missings with contacts by name
added_contacts <- miss %>% 
  inner_join(contacts, by = c(name = "contact_name")) %>% 
  group_by(name) %>% 
  mutate(n = n()) %>% 
  arrange(desc(n)) %>% 
  select(-contact_name) %>% 
  filter(n == 1) %>% # keep unique matches only
  distinct(deezer_id, .keep_all = T)

pop(added_contacts)


### MERGE INTO ALL
added_contacts <- added_contacts %>% 
  select(deezer_id, contact_id)

## before
all %>% 
  filter(!is.na(contact_id)) %>% 
  nrow()

all <- all %>% 
  left_join(added_contacts, by = c("deezer_id", "name")) %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(-c(contact_id.x, contact_id.y)) %>% 
  as_tibble()














