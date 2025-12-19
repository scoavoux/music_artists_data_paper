library(tidyverse)


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

write.csv(wiki_labels_df, "data/wiki_labels.csv")


# --------------------------------------------------------------


# outer join all 4 keys
wiki_ids <- wiki_labels_df %>% 
  
  full_join(wiki_deezer, by = "itemId") %>% 
  full_join(wiki_mbz, by = "itemId") %>% 
  # full_join(wiki_discogs, by = "itemId") %>%  # leave out for now
  # full_join(wiki_spotify, by = "itemId") %>%  # leave out for now
  
  sapply(as.character) %>% # convert all to str
  as_tibble() # as tibble

write.csv(wiki_ids, "data/wiki_ids.csv")

rm(wiki_labels, wiki_spotify, wiki_discogs, wiki_deezer,
   wiki_labels_df)

wiki_ids <- read.csv("data/wiki_ids.csv") 


# --------------------------------------------------------------
## inspect matches 

prop_na <- function(x){
  sum(is.na(x)) / length(x)
}

lapply(wiki_ids, prop_na)


# ------------------------------------------------------

# LOAD MUSICBRAINZ AND SENSCRITIQUE PAIRS
# *insert new mbz data when sam has it

mbz_deezer <- load_s3(file = "musicbrainz/mbid_deezerid.csv") %>% 
  as_tibble() %>% 
  mutate(
    musicBrainzID = as.character(mbid),
    deezerID = as.character(artist_id)
  ) %>% 
  select(-artist_id, -mbid)


mbz_spotify <- load_s3(file = "musicbrainz/mbid_spotifyid_pair.csv") %>% 
  as_tibble() %>% 
  mutate(
    musicBrainzID = as.character(mbid),
    spotifyID = as.character(spotify_id)
  ) %>% 
  select(-spotify_id, -mbid) 


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


all_ids <- wiki_ids %>% 
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
  

write_parquet(all_ids, "data/all_ids.parquet", 
              compression = "snappy")
  


# ----------------------------------------------

all_ids %>% 
  distinct(deezerID)

artists <- artists %>% 
  mutate(deezer_id = as.character(deezer_id)) %>% 
  rename(deezerID = deezer_id,
         deezer_name = name)

ids <- wiki_ids %>% 
  left_join(artists, 
            by = "deezerID")

ids$f_n_play <- ifelse(is.na(ids$f_n_play) == TRUE, 0, ids$f_n_play)

ids <- ids %>% 
  arrange(desc(f_n_play))

# 92.5% of deezerIDs
ids %>% 
  distinct(deezerID, .keep_all = T) %>% 
  tally(f_n_play)


# possible rule: compare completeness of duplicates,
# i.e. if one duplicate has all ids and the other
# only has mbz and itemId, take the first one












