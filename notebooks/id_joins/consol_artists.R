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
  select(contact_id, contact_name, mbz_id, spotify_id)


# -----------------------------------------------------------
# JOIN ALL POSSIBLE ITEM ARTISTS TO MBZ AND CONTACTS

all <- artists %>% 
  full_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  full_join(contacts, by = c(musicBrainzID = "mbz_id"))


# perfect matches between artists and mbz
# including duplicates
clean <- all %>% 
  filter(!is.na(musicBrainzID) & !is.na(contact_id) & 
           !is.na(f_n_play) & !is.na(deezer_id))

# how many % of streams are missing
mbz_missing <- all %>% 
  filter(is.na(musicBrainzID))

sum(mbz_missing$f_n_play) # 13.8%















