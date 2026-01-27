


library(sjmisc)

tar_load(all_enriched)

all_count <- all_enriched %>% 
  filter(!is.na(musicbrainz_id) & !is.na(contact_id)) %>% 
  add_count(deezer_id, name = "n_deezer") %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(contact_id, name = "n_co")


all_dups <- all_count %>% 
  filter(n_deezer > 1 |
           n_mbz > 1 |
           n_co > 1)

t <- all_dups %>% 
  distinct(deezer_id, .keep_all = T)



### MERGE PERFECT NAME DUPLICATES

#### extract them


### perfect name duplicates
### equal to deezer duplicates, logically
all_dups_name <- all_dups %>% 
  filter(n_mbz > 1 & n_deezer == 1) %>% 
  arrange(desc(musicbrainz_id))

all_dups_name %>% 
  group_by(deezer_id, name) %>% 
  summarise(contact_id = max(contact_id),
            musicbrainz_id = max(musicbrainz_id))

frq(all_dups_name$n_deezer)

