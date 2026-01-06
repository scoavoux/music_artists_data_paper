library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

# LOAD RAW -------------------------------------------

## describe data: artists only with playtime

# artists in items
tar_load(artists)
mbz_deezer <- load_s3("interim/musicbrainz_urls_collapsed.csv")
contacts <- load_s3("senscritique/contacts.csv")

# musicbrainz keys
mbz_deezer <- tibble(mbz_deezer) %>% 
  filter(!is.na(deezer)) %>% 
  rename(musicBrainzID = "musicbrainz_id",
         deezerID = "deezer") # %>% 
#select(musicBrainzID, deezerID)

# contacts keys
contacts <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name)

# wiki
wiki <- load_s3("interim/wiki_ids.csv")


# -----------------------------------------------------------
# JOIN ALL POSSIBLE ITEM ARTISTS TO MBZ AND CONTACTS

all <- artists %>% 
  full_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  full_join(contacts, by = c(musicBrainzID = "mbz_id"))


# perfect matches between artists and mbz
# including duplicates
clean <- all %>% 
  filter(!is.na(musicBrainzID) & !is.na(contact_id) & 
           !is.na(f_n_play) & !is.na(deezer_id)) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, name) # rm perfect duplicates


### MISSING IN CONTACTS
miss <- all %>% 
  filter(is.na(musicBrainzID)) %>% 
  select(deezer_id, name, musicBrainzID, f_n_play) %>% 
  distinct(name, .keep_all = T) # unique names

lapply(miss, prop_na) # 97% of which missing in mbz too, meaning we can almost never retrieve contact_id through mbz




# ----------------- NAME MATCHING miss <=> contact name

test <- miss %>% 
  inner_join(contacts, by = c(c(name = "contact_name"))) %>% 
  group_by(name) %>% 
  summarise(n = n()) %>% 
  arrange(desc(n))
  
test %>% 
  filter(n > 1)



# ---------------------- NAME MATCHING wiki

test <- miss %>% 
  inner_join(wiki, by = c(c(deezer_id = "deezerID"))) %>% 
  mutate(musicBrainzID = musicBrainzID.y)

sum(test$f_n_play)







