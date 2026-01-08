
# LOAD RAW -------------------------------------------

# artists in items
tar_load(artists)

# musicbrainz keys
mbz_deezer <- load_s3("interim/musicbrainz_urls_collapsed.csv")
mbz_deezer <- tibble(mbz_deezer) %>% 
  filter(!is.na(deezer)) %>% 
  rename(musicBrainzID = "musicbrainz_id",
         deezerID = "deezer")

# mbz names
mbz_name <- load_s3("musicbrainz/mbid_name_alias.csv") 
mbz_name <- mbz_name %>% 
  filter(type == "name") %>% 
  transmute(musicBrainzID = mbid, 
            mbz_name = name)

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
wiki <- wiki %>% 
  left_join(mbz_name, by = "musicBrainzID") %>% 
  as_tibble()


# CREATE ALL
all <- artists %>% 
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  left_join(contacts,  by = c(musicBrainzID = "mbz_id")) %>% 
  left_join(manual_search, by = "deezer_id") %>% 
  left_join(mbz_name, by = "musicBrainzID") %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(name, contact_name, mbz_name, deezer_id, 
         musicBrainzID, contact_id, f_n_play) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, .keep_all = TRUE)


## BENCHMARK ALL
pop(all)
cleanpop(all)


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
  anti_join(all, by = "contact_id")


added_contacts <- unique_name_match(
  miss = all %>% filter(is.na(contact_id)),
  ref = contacts,
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
  filter(!is.na(mbz_name)) %>% 
  anti_join(all, by = "musicBrainzID")

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

cleanpop(all) # 85.94%












