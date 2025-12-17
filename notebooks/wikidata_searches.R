
## additional operations for names not found
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



# get unique names (native name if available)
deezer_spotify <- query_wikidata("SELECT DISTINCT ?deezer_id ?spotify_id ?label
WHERE {
  ?item wdt:P2722 ?deezer_id.
  ?item wdt:P1902 ?spotify_id.

  OPTIONAL { ?item wdt:P2561 ?native_name. }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language 'en'.
    ?item rdfs:label ?label.
  }

  BIND(COALESCE(?native_name, ?label) AS ?name)
}
")


# add spotify key when there is one
keys <- wikidata_deezer %>% 
  left_join(deezer_spotify, by = c("deezer_id", "label")) %>% 
  mutate(deezer_id = as.integer(deezer_id))

mbz_wikidata <- load_s3(file = "musicbrainz/mbid_wikidataid_pair.csv") %>% 
  as_tibble()


#### figure this out: 9k NAs with mbz_wikidata as y 
#### but many more cases
keys2 <- mbz_wikidata %>% 
  left_join(keys, by = "wikidata_id")

# manual search file (senscritique / deezer_id)

# senscritique pairing (deezer API)

### Musicbrainz id / deezer id pairs from musicbrainz dumps ------
mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid_pair.csv") %>% 
  as_tibble()

str(mbz_deezer)


test <- mbz_deezer %>% 
  left_join(keys, by = "deezer_id")


