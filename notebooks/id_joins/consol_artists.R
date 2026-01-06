library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

pop <- function(x){
  sum_pop <- sum(x$f_n_play) * 100
  cat("f_n_play:", sum_pop,"%.")
}


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
           !is.na(f_n_play) & !is.na(deezer_id)) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, name, f_n_play) # rm perfect duplicates




# how many % of streams are missing
mbz_missing <- all %>% 
  filter(is.na(musicBrainzID))

sum(mbz_missing$f_n_play) # 13.8%


mbz_missing

# --------------- ADD MBZ FROM WIKI to missing in all

mbz_from_wiki <- mbz_missing %>% 
  inner_join(wiki, by = c(c(deezer_id = "deezerID"))) %>% 
  mutate(musicBrainzID = musicBrainzID.y) %>% 
  filter(!is.na(musicBrainzID)) %>% 
  distinct(deezer_id, musicBrainzID, .keep_all = T) %>% 
  select(deezer_id, name, musicBrainzID, f_n_play)

sum(mbz_from_wiki$f_n_play) # retrieving 1.8% of missing mbz ids

mbz_from_wiki$contact_id <- NA


# ADD MISSING MBZ TO CLEAN 
clean <- clean %>% 
  rbind(mbz_from_wiki)

# --> no contact_id for the new mbz cases
clean %>% 
  filter(is.na(contact_id))



# --------------- CHECK IF OTHER WIKI IDS (SPOTIFY ETC) HELP

# mbz ids who are still missing after retrieving a few from wiki
mbz_missing_after_wiki <- mbz_missing %>% 
  anti_join(mbz_from_wiki, by = "deezer_id") %>% 
  select(deezer_id, name, f_n_play)

mbz_missing_after_wiki

# matches between deezer and wiki who are still missing in mbz
# not many
t <- mbz_missing_after_wiki %>% 
  inner_join(wiki, by = c(deezer_id = "deezerID"))

# even less spotify ids
t %>% 
  filter(!is.na(spotifyID))

# even less discogs ids
t %>% 
  filter(!is.na(discogsID))


# try matching to mbz_deezer by spotify id












