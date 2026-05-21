
make_wiki_keys <- function(wiki_labels, mbz_deezer) {
  
  options(scipen = 99)
  
  # get all possible keys between deezer and musicbrainz
  
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
  
  # --------------------------------------------------------------

  # outer join all 4 keys
  wiki <- wiki_labels %>% 
    
    full_join(wiki_deezer, by = "itemId") %>% 
    full_join(wiki_mbz, by = "itemId") %>% 

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
  
  return(wiki)
  
}




