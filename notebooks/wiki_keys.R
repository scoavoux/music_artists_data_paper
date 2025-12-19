library(lubridate)
library(tidyverse)

# get all possible keys between deezer, musicbrainz, discogs, and spotify

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


# --------------------------------------------------------------

## ADD WIKI LABELS (TIME-INTENSIVE)

batch_size <- 250

all_ids <- unique(c(wiki_mbz$itemId, 
                    wiki_deezer$itemId, 
                    wiki_discogs$itemId, 
                    wiki_spotify$itemId))

# Split into batches
batches <- split(all_ids, ceiling(seq_along(all_ids)/batch_size))

wiki_labels <- vector("list", length(batches))

for (i in seq_along(batches)) {
  
  id_list <- paste0("wd:", batches[[i]])
  
  query <- sprintf(
    'SELECT ?itemId ?label WHERE {
       VALUES ?item { %s }
       BIND(STRAFTER(STR(?item), "entity/") AS ?itemId)
       OPTIONAL {
         ?item rdfs:label ?label .
         FILTER(LANG(?label) = "en")
       }
     }',
    paste(id_list, collapse = " ")
  )
  
  wiki_labels[[i]] <- query_wikidata(query)
  
  Sys.sleep(0.2)  # be nice to WDQS
}

wiki_labels_df <- bind_rows(wiki_labels) %>%
  distinct(itemId, .keep_all = TRUE)


# --------------------------------------------------------------


# outer join all 4 keys
wiki_ids <- wiki_labels_df %>% 
  
  full_join(wiki_deezer, by = "itemId") %>% 
  full_join(wiki_mbz, by = "itemId") %>% 
  full_join(wiki_discogs, by = "itemId") %>% 
  full_join(wiki_spotify, by = "itemId") %>% 
  
  sapply(as.character) %>% # convert all to str
  as_tibble() # as tibble


write.csv(wiki_ids, "data/wiki_ids.csv")

# --------------------------------------------------------------

## inspect matches 

prop_na <- function(x){
  sum(is.na(x)) / length(x)
}

lapply(wiki_ids, prop_na)


# ------------------------------------------------------

# BUILD MUSICBRAINZ TABLE
# *is template. insert new mbz data when sam has it

mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid.csv") %>% 
  as_tibble() %>% 
  rename(deezer_id = "artist_id") %>% 
  mutate(
    mbid = as.character(mbid),
    deezer_id = as.character(deezer_id)
  )

mbz_spotify <- load_s3(file = "musicbrainz/mbid_spotifyid_pair.csv") %>% 
  as_tibble()

mbz <- mbz_deezer %>% 
  full_join(mbz_spotify, by = "mbid") %>% 
  rename(musicBrainzID = "mbid",
         spotifyID = "spotify_id",
         deezerID = "deezer_id")


# ---------------------------------------------------------

# PIVOT WIKI TO LONG
wiki_long <- wiki_ids %>%
  
  select(itemId, label, musicBrainzID, deezerID, spotifyID) %>%
  pivot_longer(
    cols = c(musicBrainzID, deezerID, spotifyID),
    names_to = "id_type",
    values_to = "id_value"
  ) %>%
  filter
(!is.na(id_value))



# PIVOT MBZ TO LONG
mbz_long <- mbz %>%
  pivot_longer(
    cols = c(musicBrainzID, deezerID, spotifyID),
    names_to = "id_type",
    values_to = "id_value"
  ) %>%
  filter(!is.na(id_value))


# JOIN WIKI AND MBZ
links <- wiki_long %>%
  full_join(
    mbz_long,
    by = c("id_value", "id_type")
  )

# collapse back into wide
wiki_enriched <- links %>%
  distinct(itemId, label, id_type, id_value, mbname) %>%
  pivot_wider(
    names_from  = id_type,
    values_from = id_value,
    values_fn = first
  )

wiki_enriched # <=> WIKI + MBZ



# --------------------------------------------------
# SENSCRITIQUE

contacts <- load_s3("senscritique/contacts.csv") %>% 
  as_tibble() %>% 
  mutate(spotifyID = str_remove(spotify_id, "spotify:artist:"),
         spotifyID = ifelse(spotify_id == "", NA, spotify_id),
         contact_id = as.character(contact_id)) %>% 
  select(contact_id, 
         musicBrainzID = "mbz_id", 
         spotifyID)


# PIVOT CONTACTS TO LONG
contacts_long <- contacts %>%
  pivot_longer(
    cols = c(musicBrainzID, contact_id, spotifyID),
    names_to = "id_type",
    values_to = "id_value"
  ) %>%
  filter(!is.na(id_value))

contacts_long

wiki_enriched

wiki_enriched_long <- links %>%
  
  select(itemId, label, musicBrainzID, deezerID, spotifyID) %>%
  pivot_longer(
    cols = c(musicBrainzID, deezerID, spotifyID),
    names_to = "id_type",
    values_to = "id_value"
  ) %>%
  filter
(!is.na(id_value))



# JOIN WIKI AND MBZ
links <- wiki_long %>%
  full_join(
    mbz_long,
    by = c("id_value", "id_type")
  )

# collapse back into wide
wiki_enriched <- links %>%
  distinct(itemId, label, id_type, id_value, mbname) %>%
  pivot_wider(
    names_from  = id_type,
    values_from = id_value,
    values_fn = first
  )




















