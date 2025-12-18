# get all possible keys between deezer, musicbrainz, discogs, and spotify
# -----------------------------------------------------------

## MUSICBRAINZ

wiki_mbz <- query_wikidata("

SELECT DISTINCT
?itemId
?musicBrainzID
WHERE {
  VALUES ?type { wd:Q5 wd:Q2088357 } # Human or Musical ensemble
  ?item wdt:P31 ?type .
  
  # Optional identifiers
  ?item wdt:P434 ?musicBrainzID
  
  # Extract Wikidata ID (e.g. Q42)
  BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  
}

")


wiki_deezer <- query_wikidata("

SELECT DISTINCT
?itemId
?deezerID
WHERE {
  VALUES ?type { wd:Q5 wd:Q2088357 } # Human or Musical ensemble
  ?item wdt:P31 ?type .
  
  # Optional identifiers
  ?item wdt:P2722 ?deezerID
  
  # Extract Wikidata ID (e.g. Q42)
  BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  
}

")

wiki_discogs <- query_wikidata("

SELECT DISTINCT
?itemId
?discogsID
WHERE {
  VALUES ?type { wd:Q5 wd:Q2088357 } # Human or Musical ensemble
  ?item wdt:P31 ?type .
  
  # Optional identifiers
  ?item wdt:P1953 ?discogsID
  
  # Extract Wikidata ID (e.g. Q42)
  BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  
}

")


wiki_spotify <- query_wikidata("

SELECT DISTINCT
?itemId
?spotifyID
WHERE {
  VALUES ?type { wd:Q5 wd:Q2088357 } # Human or Musical ensemble
  ?item wdt:P31 ?type .
  
  # Optional identifiers
  ?item wdt:P1902 ?spotifyID
  
  # Extract Wikidata ID (e.g. Q42)
  BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  
}

")

# outer join all 4 keys
wiki_ids <- wiki_deezer %>% 
  full_join(wiki_discogs, by = "itemId") %>% 
  full_join(wiki_mbz, by = "itemId") %>% 
  full_join(wiki_spotify, by = "itemId")

## ADD WIKI LABELS: find a way

# remove columns with only one identifier
#df <- wiki_ids %>% 
# filter(rowSums(!is.na(select(., musicBrainzID, deezerID, discogsID, spotifyID))) >= 2)

# ------------------------------------------------------

## inspect matches 

sum(is.na(wiki_ids$musicBrainzID)) / nrow(wiki_ids)
sum(is.na(wiki_ids$discogsID)) / nrow(wiki_ids)
sum(is.na(wiki_ids$spotifyID)) / nrow(wiki_ids)
sum(is.na(wiki_ids$deezerID)) / nrow(wiki_ids)




# BUILD MUSICBRAINZ TABLE

# mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid_pair.csv") %>% 
  # as_tibble()

mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid.csv") %>% 
  as_tibble() %>% 
  rename(deezer_id = "artist_id")

mbz_spotify <- load_s3(file = "musicbrainz/mbid_spotifyid_pair.csv") %>% 
  as_tibble()

mbz_deezer
mbz_spotify



# how many rows have both id1 and id2?
wiki_ids_filled %>%
  filter(!is.na(musicBrainzID), !is.na(deezerID)) %>%
  nrow()

# 52k
wiki_ids %>%
  filter(!is.na(musicBrainzID), !is.na(deezerID)) %>%
  nrow()


##
wiki_ids_enriched <- wiki_ids %>%
  mutate(
    musicBrainzID = as.character(musicBrainzID),
    deezerID = as.character(deezerID)
  ) %>%
  
  left_join(
    mbz_deezer %>%
      mutate(
        mbid = as.character(mbid),
        deezer_id = as.character(deezer_id)
      ),
    by = c("musicBrainzID" = "mbid")
  ) %>%
  
  mutate(
    deezerID = coalesce(deezerID, deezer_id) # returns deezer_ID if not NA, otherwise deezer_id
  ) %>%
  
  select(-deezer_id)

# 110k! added ~60k pairings 
wiki_ids_enriched %>%
  filter(!is.na(musicBrainzID), !is.na(deezerID)) %>%
  nrow()

#... among which 50k new deezerIDs
sum(is.na(wiki_ids$deezerID)) - sum(is.na(wiki_ids_enriched$deezerID))

# 66k deezer artists
wiki_ids_enriched %>% 
  distinct(deezerID)



