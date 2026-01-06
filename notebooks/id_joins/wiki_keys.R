library(tidyverse)
library(arrow)
library(WikidataQueryServiceR)

options(scipen = 99)

# get all possible keys between deezer, musicbrainz, discogs, and spotify

wiki_mbz <- query_wikidata("

SELECT DISTINCT
?itemId
?musicBrainzID
WHERE {
  ?item wdt:P31/wdt:P279* ?type .
  VALUES ?type {
    wd:Q5          # human
    wd:Q215380     # musical group
  }
  
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
  ?item wdt:P31/wdt:P279* ?type .
  VALUES ?type {
    wd:Q5          # human
    wd:Q215380     # musical group
  }
  
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
  ?item wdt:P31/wdt:P279* ?type .
  VALUES ?type {
    wd:Q5          # human
    wd:Q215380     # musical group
  }
  
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
  ?item wdt:P31/wdt:P279* ?type .
  VALUES ?type {
    wd:Q5          # human
    wd:Q215380     # musical group
  }
  
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

wiki_labels <- vector("list", lQength(batches))

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

write_s3(wiki_labels_df, "interim/wiki_labels.csv")

wiki_labels_df <- load_s3("interim/wiki_labels.csv")

# --------------------------------------------------------------

# outer join all 4 keys
wiki <- wiki_labels_df %>% 
  
  full_join(wiki_deezer, by = "itemId") %>% 
  full_join(wiki_mbz, by = "itemId") %>% 
  full_join(wiki_discogs, by = "itemId") %>%  
  full_join(wiki_spotify, by = "itemId") %>%  
  
  sapply(as.character) %>% # convert all to str
  as_tibble() # as tibble

write_s3(wiki, "interim/wiki_ids.csv")

rm(wiki_labels, wiki_spotify, wiki_discogs, wiki_deezer,
   wiki_labels_df)

wiki <- load_s3("interim/wiki_ids.csv") 



# --------------------------------------------------------------
## inspect matches 

prop_na <- function(x){
  sum(is.na(x)) / length(x)
}

lapply(wiki, prop_na)

# ------------------------------------------------------

# LOAD MUSICBRAINZ NEW

mbz_deezer <- tibble(collapsed_short) %>% 
  select(-wiki) %>% 
  filter(!is.na(deezer)) %>% 
  rename(musicBrainzID = "musicbrainz_id",
         deezerID = "deezer")


## LOAD SENSCRITIQUE
contacts <- load_s3("senscritique/contacts.csv") %>% 
  as_tibble() %>% 
  mutate(spotifyID = str_remove(spotify_id, "spotify:artist:"),
         spotifyID = ifelse(spotify_id == "", NA, spotify_id),
         contact_id = as.character(contact_id)) #%>% 
  #select(contact_id, 
   #      musicBrainzID = "mbz_id",
   #       contact_name)


# ---------------------------------------------
## implement MBZ + CONTACTS in WIKI_IDs

wiki_ids_mbz <- wiki %>% 
  full_join(mbz_deezer, by = "musicBrainzID") %>% 
  mutate(deezerID = coalesce(deezerID.x, deezerID.y)) %>% 

  
  #full_join(mbz_spotify, by = "musicBrainzID") %>% 
  #mutate(spotifyID = coalesce(spotifyID.x, spotifyID.y)) %>% 
  
  #full_join(contacts, by = "musicBrainzID") %>% 

  select(-c(deezerID.x, 
            deezerID.y, 
            #spotifyID.x, 
            #spotifyID.y
            )) 

lapply(wiki_ids_mbz, prop_na)

# ------------------------------------------------------
# 

full_artists_mbz <- artists %>% 
  left_join(mbz_deezer, by = c(deezer_id = "deezerID")) %>% 
  #distinct(deezer_id, .keep_all = T) %>% 
  filter(!is.na(musicBrainzID))


test <- full_artists_mbz %>% 
  full_join(wiki, by = c(deezer_id = "deezerID")) %>% 
  mutate(musicBrainzID = coalesce(musicBrainzID.x, musicBrainzID.y)) %>% 
  select(-c(musicBrainzID.x, 
            musicBrainzID.y))


# ------------------------------------------------------

# START FROM ITEMS --> 98.5%






