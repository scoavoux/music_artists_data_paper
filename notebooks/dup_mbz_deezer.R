library(dplyr)

## IDENTIFY DUPLICATES LEADING TO DIFFERENT ARTISTS!!

## --> maybe we shouldn't care about duplicate ids so much
## and focus on duplicate ids who point to different artists
## meaning: look at name differences too!

## also: maybe create a column for duplicates to flag
## original duplicates (before the joins)?


# ----------------------- MBZ_DEEZER
tar_load(mbz_deezer)

mbz_deezer

mbz_deezer <- mbz_deezer %>% 
  filter(!is.na(deezer_id)) %>% 
  distinct(deezer_id, musicbrainz_id, .keep_all = T) %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(deezer_id, name = "n_deezer")


# multiple mbz_ids for one deezer_id
mbz_dups_deezer_id <- mbz_deezer %>% 
  filter(n_deezer > 1) %>% 
  arrange(desc(deezer_id))

# multiple deezer_ids for one mbz_id
mbz_dups_mbz_id <- mbz_deezer %>% 
  filter(!is.na(musicbrainz_id)) %>% 
  filter(n_mbz > 1) %>% 
  arrange(desc(musicbrainz_id))






















