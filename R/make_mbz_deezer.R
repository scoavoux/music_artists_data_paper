# transform the raw musicbrainz keys file from SQL to mbz_deezer
# uses data.table to collapse the urls columns into separate cols
make_mbz_deezer <- function(musicbrainz_urls) {
  
  library(stringr)
  library(data.table)
  library(dplyr)
  
  dat <- tibble(musicbrainz_urls) %>% 
    mutate(discogsID = ifelse(str_detect(url, "discogs"), 
                              str_remove(url, "https://www.discogs.com/artist/"), NA),
           allmusicID = ifelse(str_detect(url, "allmusic"), 
                               str_remove(url, "https://www.allmusic.com/artist/"), NA),
           wikiID = ifelse(str_detect(url, "wiki"), 
                           str_remove(url, "https://www.wikidata.org/wiki/"), NA),
           deezerID = ifelse(str_detect(url, "deezer"), 
                             str_remove(url, "https://www.deezer.com/artist/"), NA),
           spotifyID = ifelse(str_detect(url, "spotify"), 
                              str_remove(url, "https://open.spotify.com/artist/"), NA)) %>% 
    select(-url) %>% 
    rename(musicBrainzID = "musicbrainz_id",
           mbz_name = "artist_name") %>% 
    distinct(mbz_name, musicBrainzID, discogsID, allmusicID, 
             wikiID, deezerID, spotifyID)
  
  
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
    by = musicBrainzID
  ]
  
  
  ## clean deezerID
  recoded_http_ids <- collapsed %>% 
    filter(str_detect(deezerID, "\\D")) %>% # look for non-digits
    filter(str_detect(deezerID, "artist/")) %>% # preceded by /artist/
    mutate(deezerID = str_extract(deezerID, "\\d+")) %>%  # extract digits
    select(deezerID, musicBrainzID)
  
  # remerge into collapsed
  mbz_deezer <- collapsed %>%
    left_join(recoded_http_ids, by = "musicBrainzID") %>% 
    mutate(deezerID = coalesce(deezerID.y, deezerID.x)) %>% # take clean ID if possible
    select(-c(deezerID.x, deezerID.y))
  
  # 77 cases are simply wrong --- dropped them
  ## they lead to albums, tracks, or non-deezer profiles
  # collapsed_clean %>% 
    # filter(str_detect(deezerID, "\\D"))
  
  # write_s3(collapsed_clean, "interim/musicbrainz_urls_collapsed_new.csv")

  return(mbz_deezer)
}





