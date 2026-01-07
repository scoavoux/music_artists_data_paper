left_join_coalesce <- function(x, y, by, col) {
  x %>% 
    left_join(y, by = by, suffix = c(".x", ".y")) %>% 
    mutate("{col}" := coalesce(.data[[paste0(col, ".x")]],
                               .data[[paste0(col, ".y")]])) %>% 
    select(-all_of(c(paste0(col, ".x"), paste0(col, ".y"))))
}

unique_name_match <- function(miss, ref, name_col, id_col) {
  miss %>% 
    inner_join(ref, by = setNames(name_col, name_col)) %>% 
    group_by(.data[[name_col]]) %>% 
    filter(n() == 1) %>% 
    ungroup() %>% 
    distinct(deezer_id, .keep_all = TRUE) %>% 
    select(deezer_id, all_of(id_col))
}


all <- artists %>% 
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  left_join(contacts,  by = c(musicBrainzID = "mbz_id")) %>% 
  left_join(manual_search, by = "deezer_id") %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(name, contact_name, deezer_id, musicBrainzID, contact_id, f_n_play) %>% 
  distinct(deezer_id, musicBrainzID, contact_id, .keep_all = TRUE)



## ----- ADD WIKI-MBZ
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


## ----- ADD MBZ NAMES
mbz_name <- load_s3("musicbrainz/mbid_name_alias.csv") %>% 
  filter(type == "name") %>% 
  transmute(musicBrainzID = mbid, mbz_name = name)

all  <- all  %>% left_join(mbz_name, by = "musicBrainzID")
wiki <- wiki %>% left_join(mbz_name, by = "musicBrainzID")



## -------- ENRICH WITH CONTACTS
contacts_ref <- contacts %>% 
  select(contact_id, contact_name) %>% 
  anti_join(all, by = "contact_id")

added_contacts <- unique_name_match(
  miss = all %>% filter(is.na(contact_id)),
  ref  = contacts_ref,
  name_col = "name",
  id_col   = "contact_id"
)

all <- left_join_coalesce(
  all,
  added_contacts,
  by = "deezer_id",
  col = "contact_id"
)


## -------- ENRICH WITH MBZ FROM WIKI
wiki_ref <- wiki %>% 
  select(musicBrainzID, mbz_name) %>% 
  filter(!is.na(mbz_name)) %>% 
  anti_join(all, by = "musicBrainzID")

added_mbz <- unique_name_match(
  miss = all %>% filter(is.na(musicBrainzID)),
  ref  = wiki_ref,
  name_col = "name",
  id_col   = "musicBrainzID"
)

all <- left_join_coalesce(
  all,
  added_mbz,
  by = "deezer_id",
  col = "musicBrainzID"
)



