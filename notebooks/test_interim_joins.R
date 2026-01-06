# test the use of intermediate sources (spotify, discogs, wiki) for joins
# for testing purposes only, delete later


# ------------ LOAD DATA

# left-joined artists with mbz and contacts
all <- load_s3("interim/all_deezer_mbz_contacts.csv") %>% 
  as_tibble() %>% 
  mutate(deezer_id = as.character(deezer_id))


# --------------- CHECK IF OTHER WIKI IDS (SPOTIFY ETC) HELP

# mbz ids who are still missing after retrieving a few from wiki
# subset artists with missing mbz ids
mbz_missing_after_wiki <- all %>% 
  filter(is.na(musicBrainzID)) %>% 
  select(deezer_id, name, f_n_play)

mbz_missing_after_wiki

# matches between deezer and wiki who are still missing in mbz
# not many, around 1000
t <- mbz_missing_after_wiki %>% 
  inner_join(wiki, by = c(deezer_id = "deezerID"))


# ----- make subsets for each interim source

# even less spotify ids
spotify <- t %>% 
  filter(!is.na(spotifyID))

# even less discogs ids
discogs <- t %>% 
  filter(!is.na(discogsID))

item <- t %>% 
  filter(!is.na(itemId))


# try matching to mbz_deezer by spotify id
# very marginal number of cases
# also, these are all the same cases

mbz_deezer %>% 
  inner_join(spotify, by = c(spotify = "spotifyID"))

mbz_deezer %>% 
  inner_join(discogs, by = c(discogs = "discogsID"))

mbz_deezer %>% 
  inner_join(item, by = c(wiki = "itemId"))








