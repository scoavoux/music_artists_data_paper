library(dplyr)
library(WikidataQueryServiceR)
library(stringr)

# ARTISTS (DEEZER_ID)
## deezer_id, f_n_play, name. 705k
artists <- artists %>% 
  arrange(desc(f_n_play))

# DEEZER_ID --- SENSCRITIQUE
## deezer_id, contact_id. 189k
pairings <- read.csv("data/pairings.csv") %>% 
  as_tibble() %>% 
  select(contact_id, deezer_id)

# DEEZER_ID --- MUSICBRAINZ
## deezer_id, mbid, mbname 94k
mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid_pair.csv") %>% 
  as_tibble()

artists
pairings
mbz_deezer


# ------------------------------------------------------
# artists in senscritique

## N = 16,401 contact_ids with a match in artists
artists %>% 
  inner_join(pairings, by = "deezer_id")

## N = 15,936 unique deezer_ids
## --> ~500 cases with multiple contact_ids for one deezer_id
artists %>% 
  inner_join(pairings, by = "deezer_id") %>% 
  distinct(deezer_id, .keep_all = T)

## --> 425 cases with multiple mbids for one deezer_id
artists %>% 
  inner_join(pairings, by = "deezer_id") %>% 
  group_by(deezer_id) %>% 
  filter(n_distinct(contact_id) > 1) %>% 
  ungroup() %>% 
  distinct(deezer_id, .keep_all = T)

pairings_in_artists <- artists %>% 
  inner_join(pairings, by = "deezer_id")

# 77% of playtime
sum(pairings_in_artists$f_n_play) 


# ------------------------------------------------------
# artists in musicbrainz

## N = 55,898 contact_ids with a match in artists
artists %>% 
  inner_join(mbz_deezer, by = "deezer_id")

## N = 55,339 unique deezer_ids
artists %>% 
  inner_join(mbz_deezer, by = "deezer_id") %>% 
  distinct(deezer_id, .keep_all = T)

## --> 499 cases with multiple mbids for one deezer_id
artists %>% 
  inner_join(mbz_deezer, by = "deezer_id") %>% 
  group_by(deezer_id) %>% 
  filter(n_distinct(mbid) > 1) %>% 
  ungroup() %>% 
  distinct(deezer_id, .keep_all = T)

mbid_in_artists <- artists %>% 
  inner_join(mbz_deezer, by = "deezer_id")

# 90% of playtime
sum(mbid_in_artists$f_n_play) 




# WIKIDATA QUERIES
# ------------------------------------------------------------------

# get unique names (native name if available)
wikidata_deezer <- query_wikidata("SELECT DISTINCT ?item ?deezer_id ?label
  WHERE {
    ?item p:P2722 ?statement1.
    ?statement1 (ps:P2722) _:anyValueP2722.
    ?item wdt:P2722 ?deezer_id.
    
      OPTIONAL { ?item wdt:P2561 ?native_name. }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language 'en'.
    ?item rdfs:label ?label.
  }

  BIND(COALESCE(?native_name, ?label) AS ?name)
  }") %>% 
  mutate(wikidata_id = str_extract(item, "/(Q\\d+)", group = 1)) %>% 
  select(-item)






















