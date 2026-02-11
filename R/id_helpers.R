# helper functions for the artist id issues 

# quick benchmark of stream shares
# input x is a dataframe with cols deezer_id, musicbrainz_id, contact_id
print_stream_share <- function(x){ 
  
  require(dplyr)

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

  if(length(x$n_ratings) != 0){
    mbz_sc_rating <- x %>% 
      filter(!is.na(mbz_artist_id)) %>% 
      filter(!is.na(sc_artist_id)) %>% 
      filter(!is.na(n_ratings)) %>% 
      distinct(dz_artist_id, .keep_all = T)
  }
  else{
    mbz_sc_rating <- mbz_sc
  }
  
  
  
  mbz_clean <- sum(mbz$dz_stream_share)
  sc_clean <- sum(sc$dz_stream_share)
  mbz_sc_clean <- sum(mbz_sc$dz_stream_share)
  mbz_sc_rating_clean <- sum(mbz_sc_rating$dz_stream_share)
  
  deezer <- x %>% 
    distinct(dz_artist_id, dz_stream_share)
  deezer_clean <- sum(deezer$dz_stream_share)
  
  dat <- tibble(clean_ids = c("musicbrainz:", 
                              "senscritique:", 
                              "clean:",
                              "clean with ratings:",
                              "N total:"),
         
         stream_share = c(mbz_clean, 
                          sc_clean, 
                          mbz_sc_clean, 
                          mbz_sc_rating_clean,
                          deezer_clean),
         
         N = c(nrow(mbz), 
               nrow(sc), 
               nrow(mbz_sc), 
               nrow(mbz_sc_rating), 
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
make <- function(){
  
  require(targets)
  require(tarchetypes)
  
  tar_source("R")
  
  tar_make()
  
}








