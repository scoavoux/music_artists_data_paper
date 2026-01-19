# transform the raw musicbrainz keys file from SQL to mbz_deezer
# uses data.table to collapse the urls columns into separate cols
make_mbz_deezer <- function(musicbrainz_urls) {
  
  library(stringr)
  library(data.table)
  library(dplyr)
  
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
    select(deezer_id, musicbrainz_id)
  
  # remerge into collapsed
  mbz_deezer <- collapsed %>%
    as_tibble() %>% 
    left_join(recoded_http_ids, by = "musicbrainz_id") %>% 
    mutate(deezer_id = coalesce(deezer_id.y, deezer_id.x)) %>% # take clean ID if possible
    select(musicbrainz_id, deezer_id, mbz_name) # only keep relevant ids + name
  
  # 77 cases are simply wrong --- dropped them
  ## they lead to albums, tracks, or non-deezer profiles
  # collapsed_clean %>% 
    # filter(str_detect(deezer_id, "\\D"))
  
  # write_s3(collapsed_clean, "interim/musicbrainz_urls_collapsed_new.csv")

  return(mbz_deezer)
}















