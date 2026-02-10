# helper functions for the artist id issues 

# quick benchmark of stream shares
# input x is a dataframe with cols deezer_id, musicbrainz_id, contact_id
cleanpop <- function(x){ 
  
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
  
  
  
  mbz_clean <- sum(mbz$pop)
  sc_clean <- sum(sc$pop)
  mbz_sc_clean <- sum(mbz_sc$pop)
  mbz_sc_rating_clean <- sum(mbz_sc_rating$pop)
  
  deezer <- x %>% 
    distinct(dz_artist_id, pop)
  deezer_clean <- sum(deezer$pop)
  
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
pop <- function(x, 
                deezer=artists){
  
  require(stringr)
  
  patch <- deezer %>% 
    inner_join(x, by = "deezer_id") %>% 
    distinct(deezer_id, .keep_all = T)
  
  len <- nrow(patch)
  streams <- round(sum(patch$pop), digits = 4)
  
  print(str_glue("N: {len} \n"))
  print(str_glue("pop: {streams} % \n"))
  
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




# t <- all_enriched %>% 
#   rows_update(ratings, by = "contact_id")
# 
# 
# t <- all_enriched %>% 
#   filter(!is.na(musicbrainz_id)) %>% 
#   filter(!is.na(contact_id)) %>% 
#   filter(!is.na(rating))







