library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

pop <- function(x){
  sum_pop <- sum(x$f_n_play) * 100
  cat("f_n_play:",sum_pop,"%.")
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
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  left_join(contacts, by = c(musicBrainzID = "mbz_id")) %>% 
  select(name, contact_name, deezer_id, musicBrainzID, contact_id, f_n_play) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, .keep_all = T)




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

sum(mbz_from_wiki$f_n_play) # retrieving 1.8% of missing mbz ids --- immerhin


mbz_from_wiki <- mbz_from_wiki %>% 
  mutate(contact_id = NA) %>% 
  select(deezer_id, musicBrainzID)


# merge missing mbz ids back into "all" and save
all <- all %>% 
  left_join(mbz_from_wiki, by = "deezer_id") %>% 
  mutate(musicBrainzID = coalesce(musicBrainzID.x,
                                  musicBrainzID.y)) %>% 
  select(-c(musicBrainzID.x, musicBrainzID.y))


# write "all" with added mbz ids from wiki to a csv
write_s3("interim/clean_deezer_mbz_contacts.csv")




# ----- OPTIONAL: extract clean joins

# perfect matches between artists and mbz (including duplicates)
clean <- all %>% 
  filter(!is.na(musicBrainzID) & !is.na(contact_id) & 
           !is.na(f_n_play) & !is.na(deezer_id)) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, name, f_n_play) # rm perfect duplicates















