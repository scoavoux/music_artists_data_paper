## insert custom functions to clean the raw files here

## select relevant columns, filter duplicates,
## remove unused cases, recode ids to str, recode wrong ids (https...), etc.


## ADD DUPLICATE SOLVING HERE!


# ---------------- MANUAL SEARCH

load_manual_search <- function(file){
  
  manual_search <- read.csv(file)
  
  manual_search <- manual_search %>% 
    rename(dz_artist_id = "artist_id",
           sc_artist_id = "contact_id") %>% 
    mutate_if(is.integer, as.character) %>% 
    distinct(dz_artist_id, sc_artist_id) %>% 
    as_tibble()
  
  ## manual searches by paul
  ## INTEGRATE TO MANUAL_SEARCH!
  manual_sc_1 <- load_s3("interim/missings_to_handcode/06.02-handcoded_contacts.csv")
  
  manual_sc_2 <- load_s3("interim/missings_to_handcode/handcoded_09.02.csv")
  
  manual_sc_1 <- manual_sc_1 %>% 
    as_tibble() %>% 
    filter(!is.na(sc_artist_id)) %>% 
    mutate_if(is.integer, as.character) %>% 
    select(dz_artist_id, sc_artist_id) 
  
  manual_sc_2 <- manual_sc_2 %>% 
    as_tibble() %>% 
    filter(!is.na(contact_id)) %>% 
    mutate_if(is.integer, as.character) %>% 
    select(dz_artist_id = "deezer_id", 
           sc_artist_id = "contact_id") 
  
  # manual mbz
  
  mbz_manual_1 <- load_s3("interim/missings_to_handcode/missing_mbz_1202.csv")
  mbz_manual_1 <- mbz_manual_1 %>% 
    filter(!is.na(mbz_artist_id)) %>% 
    mutate_if(is.integer, as.character)
  
  manual_search <- manual_search %>% 
    bind_rows(manual_sc_1) %>% 
    bind_rows(manual_sc_2) %>% 
    full_join(mbz_manual_1, by = "dz_artist_id") %>% 
    distinct(dz_artist_id, sc_artist_id, mbz_artist_id)
  
  return(manual_search)
}



# ----------------- RATINGS ---------------------------

load_sc_ratings <- function(sc_ratings_file, sc_albums_file){
  
  ratings <- load_s3(sc_ratings_file)
  sc_alb <- load_s3(sc_albums_file)
  
  ratings <- ratings %>% 
    group_by(product_id) %>% 
    summarise(rating = sum(rating)) %>% 
    inner_join(sc_alb, by = "product_id") %>% 
    group_by(contact_id) %>% 
    summarise(n_ratings = sum(rating)) %>% 
    filter(!is.na(n_ratings)) %>% 
    mutate(sc_artist_id = as.character(contact_id)) %>% 
    select(sc_artist_id, n_ratings)
  
  return(ratings)
  
}



# ----------------- SENSCRITIQUE ---------------------------

load_senscritique <- function(sc_file){
  
  require(dplyr)
  require(stringr)
  
  senscritique <- load_s3(sc_file)

  clean_senscritique <- senscritique %>% 
    
    # set "" names as NA
    mutate(
      sc_artist_id = contact_id,
      mbz_artist_id = na_if(mbz_id, ""),
      sc_name = contact_name,
      sc_name = na_if(sc_name, ""),
      sc_collection_count = ifelse(is.na(collection_count), 0, collection_count)
      ) %>% 
    
    # id cols to character for clean joins
    mutate(sc_artist_id = as.character(sc_artist_id),
           mbz_artist_id = as.character(mbz_artist_id)) %>%
    
    # clean (2) dirty ids
    mutate(mbz_id = str_remove(mbz_id, "https://musicbrainz.org/artist/")) %>%
    
  select(sc_artist_id, 
         sc_name, 
         sc_collection_count, 
         mbz_artist_id) %>% 
    
  as_tibble()
  
  return(clean_senscritique)
  
}


# ----------------- MBZ-DEEZER ---------------------------

# transform the raw musicbrainz keys file from SQL to mbz_deezer
# uses data.table to collapse the urls columns into separate cols
load_mbz_deezer <- function(file) {
  
  require(stringr)
  require(data.table)
  require(dplyr)
  
  musicbrainz_urls <- load_s3(file) # load musicbrainz urls
  
  dat <- tibble(musicbrainz_urls) %>% 
    mutate(discogs_artist_id = ifelse(str_detect(url, "discogs"), 
                               str_remove(url, "https://www.discogs.com/artist/"), NA),
           allmusic_artist_id = ifelse(str_detect(url, "allmusic"), 
                                str_remove(url, "https://www.allmusic.com/artist/"), NA),
           wiki_artist_id = ifelse(str_detect(url, "wiki"), 
                            str_remove(url, "https://www.wikidata.org/wiki/"), NA),
           dz_artist_id = ifelse(str_detect(url, "deezer"), 
                              str_remove(url, "https://www.deezer.com/artist/"), NA),
           spotify_artist_id = ifelse(str_detect(url, "spotify"), 
                               str_remove(url, "https://open.spotify.com/artist/"), NA)) %>% 
    select(-url) %>% 
    rename(mbz_name = "artist_name",
           mbz_artist_id = "musicbrainz_id") %>% 
    distinct(mbz_name, mbz_artist_id, discogs_artist_id, allmusic_artist_id, 
             wiki_artist_id, dz_artist_id, spotify_artist_id)
  
  
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
    by = mbz_artist_id
  ]
  
  
  
  ## clean deezer_id
  
  recoded_http_ids <- collapsed %>% 
    filter(str_detect(dz_artist_id, "\\D")) %>% # look for non-digits
    filter(str_detect(dz_artist_id, "artist/")) %>% # preceded by /artist/
    mutate(dz_artist_id = str_extract(dz_artist_id, "\\d+")) %>%  # extract digits
    filter(!str_detect(dz_artist_id, "https")) %>%  # remove rest of URLs
    select(dz_artist_id, mbz_artist_id)
  
  # remerge into collapsed
  mbz_deezer <- collapsed %>%
    as_tibble() %>% 
    left_join(recoded_http_ids, by = "mbz_artist_id") %>% 
    mutate(dz_artist_id = coalesce(dz_artist_id.y, dz_artist_id.x)) %>% # take clean ID if possible
    mutate_if(is.integer, as.character) %>% # ids to str for clean joins
    select(mbz_artist_id, dz_artist_id, mbz_name) # only keep relevant ids + name
  
  # 77 cases are simply wrong --- dropped them
  ## they lead to albums, tracks, or non-deezer profiles
  # collapsed_clean %>% 
  # filter(str_detect(deezer_id, "\\D"))
  
  # write_s3(collapsed_clean, "interim/musicbrainz_urls_collapsed_new.csv")
  
  return(mbz_deezer)
}


# ------------------- WIKI ---------------------------
load_wiki <- function(mbz_deezer) {
  
  # require(WikidataQueryServiceR)
  # 
  # options(scipen = 99)
  # 
  # # get all possible keys between deezer, musicbrainz, discogs, and spotify
  # 
  # wiki_mbz <- query_wikidata("
  # 
  # SELECT DISTINCT
  # ?itemId
  # ?musicBrainzID
  # WHERE {
  #   ?item wdt:P31/wdt:P279* ?type .
  #   VALUES ?type {
  #     wd:Q5          # human
  #     wd:Q215380     # musical group
  #   }
  #   
  #   # Optional identifiers
  #   ?item wdt:P434 ?musicBrainzID
  #   
  #   # Extract Wikidata ID (e.g. Q42)
  #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  #   
  # }
  # 
  # ")
  # 
  # 
  # wiki_deezer <- query_wikidata("
  # 
  # SELECT DISTINCT
  # ?itemId
  # ?deezerID
  # WHERE {
  #   ?item wdt:P31/wdt:P279* ?type .
  #   VALUES ?type {
  #     wd:Q5          # human
  #     wd:Q215380     # musical group
  #   }
  #   
  #   # Optional identifiers
  #   ?item wdt:P2722 ?deezerID
  #   
  #   # Extract Wikidata ID (e.g. Q42)
  #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  #   
  # }
  # 
  # ")
  # 
  # # wiki_discogs <- query_wikidata("
  # # 
  # # SELECT DISTINCT
  # # ?itemId
  # # ?discogsID
  # # WHERE {
  # #   ?item wdt:P31/wdt:P279* ?type .
  # #   VALUES ?type {
  # #     wd:Q5          # human
  # #     wd:Q215380     # musical group
  # #   }
  # #   
  # #   # Optional identifiers
  # #   ?item wdt:P1953 ?discogsID
  # #   
  # #   # Extract Wikidata ID (e.g. Q42)
  # #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  # #   
  # # }
  # # 
  # # ")
  # # 
  # # 
  # # wiki_spotify <- query_wikidata("
  # # 
  # # SELECT DISTINCT
  # # ?itemId
  # # ?spotifyID
  # # WHERE {
  # #   ?item wdt:P31/wdt:P279* ?type .
  # #   VALUES ?type {
  # #     wd:Q5          # human
  # #     wd:Q215380     # musical group
  # #   }
  # #   
  # #   # Optional identifiers
  # #   ?item wdt:P1902 ?spotifyID
  # #   
  # #   # Extract Wikidata ID (e.g. Q42)
  # #   BIND(STRAFTER(STR(?item), 'entity/') AS ?itemId)
  # #   
  # # }
  # # 
  # # ")
  # # 
  # 
  # # --------------------------------------------------------------
  # 
  # ## ADD WIKI LABELS
  # # TIME-INTENSIVE, uncomment some time later to rerun
  # 
  # batch_size <- 250
  # 
  # all_ids <- unique(c(wiki_mbz$itemId,
  #                     wiki_deezer$itemId))
  # 
  # # Split into batches
  # batches <- split(all_ids, ceiling(seq_along(all_ids)/batch_size))
  # 
  # wiki_labels <- vector("list", length(batches))
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
  # --------------------------------------------------------------
  

  
  # # outer join all 4 keys
  # wiki <- wiki_labels %>% 
  #   
  #   full_join(wiki_deezer, by = "itemId") %>% 
  #   full_join(wiki_mbz, by = "itemId") 
  #   #full_join(wiki_discogs, by = "itemId") %>%  
  #   #full_join(wiki_spotify, by = "itemId") %>%  
  #   
  #
  
  ## TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP
  
  wiki <- load_s3("interim/wiki_ids.csv")
  
  ## TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP -- TEMP
  
  #
  # 
  # ADD MBZ NAMES
  mbz_name <- mbz_deezer %>% 
    select(mbz_artist_id, mbz_name)
  
  wiki <- wiki %>% 
    rename(dz_artist_id = "deezerID",
           mbz_artist_id = "musicBrainzID",
           wiki_name = "label") %>% 
    left_join(mbz_name, by = "mbz_artist_id") %>% 
    mutate_if(is.integer, as.character) %>% # ids to str for clean joins
    mutate_if(is.double, as.character) %>% # ids to str for clean joins
    as_tibble()
  
  return(wiki)
  
}

