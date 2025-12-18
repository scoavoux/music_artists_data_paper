
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













