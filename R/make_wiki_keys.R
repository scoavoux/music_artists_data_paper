
make_wiki_keys <- function(wiki_labels, mbz_deezer) {
  
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
  
  # wiki_discogs <- query_wikidata("
  # 
  # SELECT DISTINCT
  # ?itemId
  # ?discogsID
  # WHERE {
  #   ?item wdt:P31/wdt:P279* ?type .
  #   VALUES ?type {
  #     wd:Q5          # human
  #     wd:Q215380     # musical group
  #   }
  #   
  #   # Optional identifiers
  #   ?item wdt:P1953 ?discogsID
  #   
  #   # Extract Wikidata ID (e.g. Q42)
  #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  #   
  # }
  # 
  # ")
  # 
  # 
  # wiki_spotify <- query_wikidata("
  # 
  # SELECT DISTINCT
  # ?itemId
  # ?spotifyID
  # WHERE {
  #   ?item wdt:P31/wdt:P279* ?type .
  #   VALUES ?type {
  #     wd:Q5          # human
  #     wd:Q215380     # musical group
  #   }
  #   
  #   # Optional identifiers
  #   ?item wdt:P1902 ?spotifyID
  #   
  #   # Extract Wikidata ID (e.g. Q42)
  #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  #   
  # }
  # 
  # ")
  # 
  
  # --------------------------------------------------------------
  
  ## ADD WIKI LABELS
  # TIME-INTENSIVE, uncomment some time later to rerun 
  # (ctrl + shift + C!)
  
  # batch_size <- 250
  # 
  # all_ids <- unique(c(wiki_mbz$itemId, 
  #                     wiki_deezer$itemId, 
  #                     wiki_discogs$itemId, 
  #                     wiki_spotify$itemId))
  # 
  # # Split into batches
  # batches <- split(all_ids, ceiling(seq_along(all_ids)/batch_size))
  # 
  # wiki_labels <- vector("list", lQength(batches))
  # 
  # for (i in seq_along(batches)) {
  #   
  #   id_list <- paste0("wd:", batches[[i]])
  #   
  #   query <- sprintf(
  #     'SELECT ?itemId ?label WHERE {
  #        VALUES ?item { %s }
  #        BIND(STRAFTER(STR(?item), "entity/") AS ?itemId)
  #        OPTIONAL {
  #          ?item rdfs:label ?label .
  #          FILTER(LANG(?label) = "en")
  #        }
  #      }',
  #     paste(id_list, collapse = " ")
  #   )
  #   
  #   wiki_labels[[i]] <- query_wikidata(query)
  #   
  #   Sys.sleep(0.2)  # be nice to WDQS
  # }
  # 
  # wiki_labels <- bind_rows(wiki_labels) %>%
  #   distinct(itemId, .keep_all = TRUE) %>% 
  #   as_tibble()
  # 
  # write_s3(wiki_labels, "interim/wiki_labels.csv")
  
  # --------------------------------------------------------------

  # outer join all 4 keys
  wiki <- wiki_labels %>% 
    
    full_join(wiki_deezer, by = "itemId") %>% 
    full_join(wiki_mbz, by = "itemId") %>% 
    #full_join(wiki_discogs, by = "itemId") %>%  
    #full_join(wiki_spotify, by = "itemId") %>%  
    
    sapply(as.character) %>% # convert all to str
    as_tibble() # as tibble
  
  
  
  # ------------------------------------------------------
  
  # ADD MBZ NAMES
  
  mbz_name <- mbz_deezer %>% 
    select(musicbrainz_id, mbz_name)
  
  wiki <- wiki %>% 
    rename(deezer_id = "deezerID",
           musicbrainz_id = "musicBrainzID",
           wiki_name = "label") %>% 
    left_join(mbz_name, by = "musicbrainz_id") %>% 
    as_tibble()
  
  # write_s3(wiki, "interim/wiki_ids.csv")
  
  
  return(wiki)
  
}




