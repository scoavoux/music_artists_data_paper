

tar_load(all_enriched)

all_count <- all_enriched %>% 
  filter(!is.na(musicbrainz_id) | !is.na(contact_id)) %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(contact_id, name = "n_co")


all_dups <- all_count %>% 
  filter(n_deezer > 1 |
           n_mbz > 1 |
           n_co > 1)


View(all_dups)

### perfect name duplicates
all_dups %>% 
  group_by(deezer_id, name) %>% 
  add_count() %>% 
  filter(n > 1)


string <- c("&", " et ", " and")

all_enriched %>% 
  filter(str_detect(name, string) | str_detect(" and ") | str_detect(" et "))


















