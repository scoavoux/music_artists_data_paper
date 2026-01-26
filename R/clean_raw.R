## insert custom functions to clean the raw files here

## select relevant columns, filter duplicates,
## remove unused cases, recode ids to str, recode wrong ids (https...), etc.


## ADD DUPLICATE SOLVING HERE!


# ---------------- MANUAL SEARCH

load_manual_search <- function(file){
  
  manual_search <- read.csv(file)
  
  manual_search <- manual_search %>% 
    rename(deezer_id = "artist_id") %>% 
    distinct(deezer_id, contact_id) %>% 
    as_tibble()
  
  return(manual_search)
}



# ----------------- CONTACTS ---------------------------

load_contacts <- function(file){
  
  require(dplyr)
  require(stringr)
  
  contacts <- load_s3(file)
  
  clean_contacts <- contacts %>% 
    
    # set "" names as NA
    mutate(
      mbz_id = na_if(mbz_id, ""),
      contact_name = na_if(contact_name, "") 
    ) %>% 
    
    # id cols to character for clean joins
    mutate_if(is.integer, as.character) %>%
    
    # clean (2) dirty ids
    mutate(mbz_id = str_remove(mbz_id, "https://musicbrainz.org/artist/")) %>%  

    rename(musicbrainz_id = "mbz_id") %>% 
      
  select(contact_id, contact_name, collection_count, musicbrainz_id) %>% 
  as_tibble()
  
  return(clean_contacts)
  
}


# ----------------- MBZ-DEEZER ---------------------------

# transform the raw musicbrainz keys file from SQL to mbz_deezer
# uses data.table to collapse the urls columns into separate cols
load_mbz_deezer <- function(file) {
  
  require(stringr)
  require(data.table)
  require(dplyr)
  
  dat <- load_s3(file) # load musicbrainz urls
  
  dat <- tibble(musicbrainz_urls) %>% 
    mutate(discogs_id = ifelse(str_detect(url, "discogs"), 
                               str_remove(url, "https://www.discogs.com/artist/"), NA),
           allmusic_id = ifelse(str_detect(url, "allmusic"), 
                                str_remove(url, "https://www.allmusic.com/artist/"), NA),
           wiki_id = ifelse(str_detect(url, "wiki"), 
                            str_remove(url, "https://www.wikidata.org/wiki/"), NA),
           deezer_id = ifelse(str_detect(url, "deezer"), 
                              str_remove(url, "https://www.deezer.com/artist/"), NA),
           spotify_id = ifelse(str_detect(url, "spotify"), 
                               str_remove(url, "https://open.spotify.com/artist/"), NA)) %>% 
    select(-url) %>% 
    rename(mbz_name = "artist_name") %>% 
    distinct(mbz_name, musicbrainz_id, discogs_id, allmusic_id, 
             wiki_id, deezer_id, spotify_id)
  
  
  dat <- setDT(dat)
  
  
  collapsed <- dat[
    ,
    {
      # For each non-musicbrainz column, get unique non-NA values
      vals <- lapply(.SD, function(x) unique(x[!is.na(x)]))
      
      # Replace empty with NA, singletons with 1 value, multiples stay as-is
      vals <- lapply(vals, function(x) {
        if (length(x) == 0) NA_character_
        else x
      })
      
      # Expand only where needed (cartesian product)
      as.data.table(do.call(CJ, c(vals, sorted = FALSE)))
    },
    by = musicbrainz_id
  ]
  
  
  
  ## clean deezer_id
  
  recoded_http_ids <- collapsed %>% 
    filter(str_detect(deezer_id, "\\D")) %>% # look for non-digits
    filter(str_detect(deezer_id, "artist/")) %>% # preceded by /artist/
    mutate(deezer_id = str_extract(deezer_id, "\\d+")) %>%  # extract digits
    filter(!str_detect(deezer_id, "https")) # remove rest of URLs
    select(deezer_id, musicbrainz_id)
  
  # remerge into collapsed
  mbz_deezer <- collapsed %>%
    as_tibble() %>% 
    left_join(recoded_http_ids, by = "musicbrainz_id") %>% 
    mutate(deezer_id = coalesce(deezer_id.y, deezer_id.x)) %>% # take clean ID if possible
    mutate_if(is.integer, as.character) %>% # ids to str for clean joins
    select(musicbrainz_id, deezer_id, mbz_name) # only keep relevant ids + name
  
  # 77 cases are simply wrong --- dropped them
  ## they lead to albums, tracks, or non-deezer profiles
  # collapsed_clean %>% 
  # filter(str_detect(deezer_id, "\\D"))
  
  # write_s3(collapsed_clean, "interim/musicbrainz_urls_collapsed_new.csv")
  
  return(mbz_deezer)
}


# ------------------- WIKI ---------------------------

load_wiki <- function(wiki_labels, mbz_deezer) {
  
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
  
  # --------------------------------------------------------------
  
  # outer join all 4 keys
  wiki <- wiki_labels %>% 
    
    full_join(wiki_deezer, by = "itemId") %>% 
    full_join(wiki_mbz, by = "itemId") 
    #full_join(wiki_discogs, by = "itemId") %>%  
    #full_join(wiki_spotify, by = "itemId") %>%  
    
  
  # ADD MBZ NAMES
  mbz_name <- mbz_deezer %>% 
    select(musicbrainz_id, mbz_name)
  
  wiki <- wiki %>% 
    rename(deezer_id = "deezerID",
           musicbrainz_id = "musicBrainzID",
           wiki_name = "label") %>% 
    left_join(mbz_name, by = "musicbrainz_id") %>% 
    mutate_if(is.integer, as.character) %>% # ids to str for clean joins
    as_tibble()
  
  return(wiki)
  
}








