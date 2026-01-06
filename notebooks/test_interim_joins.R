


all <- load_s3("interim/all_deezer_mbz_contacts.csv")

# missing in mbz
mbz_missing <- all %>% 
  filter(is.na(musicBrainzID))

pop(mbz_missing)



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
spotify <- t %>% 
  filter(!is.na(spotifyID))

# even less discogs ids
discogs <- t %>% 
  filter(!is.na(discogsID))

item <- t %>% 
  filter(!is.na(itemId))


# try matching to mbz_deezer by spotify id
# very marginal number of cases

mbz_deezer %>% 
  inner_join(spotify, by = c(spotify = "spotifyID"))

mbz_deezer %>% 
  inner_join(discogs, by = c(discogs = "discogsID"))

mbz_deezer %>% 
  inner_join(item, by = c(wiki = "itemId"))


# ----------------------------- 
