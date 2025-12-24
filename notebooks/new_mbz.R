library(stringr)

dat <- load_s3("musicbrainz/musicbrainz_urls.csv")

dat <- tibble(dat) %>% 
  mutate(discogs = ifelse(str_detect(url, "discogs"), 
                          str_remove(url, "https://www.discogs.com/artist/"), NA),
         allmusic = ifelse(str_detect(url, "allmusic"), 
                           str_remove(url, "https://www.allmusic.com/artist/"), NA),
         wiki = ifelse(str_detect(url, "wiki"), 
                       str_remove(url, "https://www.wikidata.org/wiki/"), NA),
         deezer = ifelse(str_detect(url, "deezer"), 
                         str_remove(url, "https://www.deezer.com/artist/"), NA),
         spotify = ifelse(str_detect(url, "spotify"), 
                          str_remove(url, "https://open.spotify.com/artist/"), NA)) %>% 
  select(-url) %>% 
  distinct(musicbrainz_id, discogs, allmusic, wiki, deezer, spotify)


library(data.table)

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


write_parquet(collapsed, "data/musicbrainz_urls_collapsed.parquet")


collapsed_short <- collapsed %>% 
  select(-c(spotify, allmusic, discogs)) %>% 
  distinct(musicbrainz_id, wiki, deezer)
  


wiki_ids <- wiki_ids %>% 
  mutate(deezerID = as.character(deezerID),
         itemId = as.character(itemId),
         musicBrainzID = as.character(musicBrainzID))


test <- wiki_ids %>% 
  inner_join(collapsed_short, by = c(deezerID = "deezer",
                                     itemId = "wiki",
                                     musicBrainzID = "musicbrainz_id"))











