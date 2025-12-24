library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

# artists in items
tar_load(artists)

# musicbrainz keys
mbz_deezer <- tibble(collapsed_short) %>% 
  select(-wiki) %>% 
  filter(!is.na(deezer)) %>% 
  rename(musicBrainzID = "musicbrainz_id",
         deezerID = "deezer")

contacts <- contacts %>% 
  select(contact_id, contact_name, mbz_id)



# -----------------------------------------------------------
# JOIN ALL POSSIBLE ITEM ARTISTS TO A MBZ ID

# join all possible artists with playtime to a mbz id
all <- artists %>% 
  full_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  full_join(contacts, by = c(musicBrainzID = "mbz_id"))


# perfect matches between artists and mbz
clean <- all %>% 
  filter(!is.na(musicBrainzID) & !is.na(contact_id) & 
           !is.na(f_n_play) & !is.na(deezer_id))


mbz_missing <- all %>% 
  filter(is.na(musicBrainzID))

sum(missing_in_mbz$f_n_play) # 13.8% of streams


















