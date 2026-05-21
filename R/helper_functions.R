# helper functions for the artist id issues 

# quick benchmark of stream shares after consolidatoin steps
# input x is a dataframe with the 3 ids columns (and optionnally n_ratings)
# output is a tibble with N and stream_share covered by each id
print_stream_share <- function(x){ 
  
  require(dplyr)
  
  x <- x %>% 
    mutate(dz_stream_share = (n_plays / sum(n_plays, na.rm = T)) * 100)

  mbz <- x %>% 
    filter(!is.na(mbz_artist_id)) %>% 
    distinct(dz_artist_id, .keep_all = T)

  sc <- x %>%
    filter(!is.na(sc_artist_id)) %>% 
    distinct(dz_artist_id, .keep_all = T)

  mbz_sc <- x %>%
    filter(!is.na(mbz_artist_id)) %>% 
    filter(!is.na(sc_artist_id)) %>% 
    distinct(dz_artist_id, .keep_all = T)

  mbz_clean <- sum(mbz$dz_stream_share, na.rm = T)
  sc_clean <- sum(sc$dz_stream_share, na.rm = T)
  mbz_sc_clean <- sum(mbz_sc$dz_stream_share, na.rm = T)

  deezer <- x %>% 
    distinct(dz_artist_id, dz_stream_share)
  deezer_clean <- sum(deezer$dz_stream_share, na.rm = T)
  
  dat <- tibble(clean_ids = c("musicbrainz:", 
                              "senscritique:", 
                              "clean:",
                              "N total:"),
         
         stream_share = c(mbz_clean, 
                          sc_clean, 
                          mbz_sc_clean, 
                          deezer_clean),
         
         N = c(nrow(mbz), 
               nrow(sc), 
               nrow(mbz_sc), 
               nrow(deezer)))

  print(dat)
  
}

# show stream share of patches
stream_share_patch <- function(x, 
                deezer = artists){
  
  require(stringr)
  
  patch <- deezer %>% 
    inner_join(x, by = "deezer_id") %>% 
    distinct(deezer_id, .keep_all = T)
  
  len <- nrow(patch)
  streams <- round(sum(patch$dz_stream_share), digits = 4)
  
  print(str_glue("N: {len} \n"))
  print(str_glue("dz_stream_share: {streams} % \n"))
  
}


# prop of nas of each column within a dataset
prop_na <- function(x) {
  
  nas <- function(x){
    sum(is.na(x)) / length(x)
  }
  
  lapply(x, nas)
  
}

# wrapper for tar_source("R") and tar_make()
# (maybe i'm just super lazy but that's alright)
make <- function(){
  
  require(targets)
  require(tarchetypes)
  
  tar_source("R")
  
  tar_make()
  
}

str_normalize <- function(str){
  
  require(stringr)
  require(stringi)
  
  
  str <- str %>% 
    
    str_to_lower() %>%  
    
    stri_trans_general("Latin-ASCII") %>% # rm accents
    
    str_replace_all(c(
      #"\\bthe\\b" = "(the|les|des|du|de\sla)?", # LEAVE OUT FOR NOW
      "\\b(the|les|des|le|la)\\s\\b" = "", # remove the
      "\\b(?:and|et|&)\\b" = "&", # unify &
      "-" = " "
    )) %>% 
    
    str_remove_all("[.,!?;:]") %>% 
    
    str_squish() # trim + remove extra spaces
  
  return(str)
}


# clean profession descriptions for isei
normalize_job <- function(string) {
  
  require(stringi)
  
  string %>%
    str_to_lower() %>% # tolower
    stri_trans_general("Latin-ASCII") %>%  # rm accents
    
    str_remove_all("[.,!?;:]") %>% 
    
    str_remove_all("\\b(de|du|des|la|le|l'|en|et|d')\\b") %>% 
    
    str_squish() # rm whitespaces
  
}













