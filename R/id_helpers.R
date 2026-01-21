# helper functions for the artist id issues 

# quick benchmark of stream shares
# input x is a dataframe with cols deezer_id, musicbrainz_id, contact_id
cleanpop <- function(x){ 
  
  require(dplyr)

  mbz <- x %>% 
    filter(!is.na(musicbrainz_id)) %>% 
    distinct(deezer_id, .keep_all = T)

  contacts <- x %>%
    filter(!is.na(contact_id)) %>% 
    distinct(deezer_id, .keep_all = T)
  
  both <- x %>%
    filter(!is.na(musicbrainz_id)) %>% 
    filter(!is.na(contact_id)) %>% 
    distinct(deezer_id, .keep_all = T)
  
  mbz_clean <- sum(mbz$pop)
  contacts_clean <- sum(contacts$pop)
  both_clean <- sum(both$pop)
  
  #cat("N:",nrow(x %>% distinct(deezer_id)),"\n")
  #cat("clean mbz ids:",mbz_clean,"% // N:",nrow(mbz),"\n")
  #cat("clean contact ids:",contacts_clean,"% // N:",nrow(contacts),"\n")
  #cat("complete cases:",both_clean,"% // N:",nrow(both),"\n")
  
  dat <- tibble(clean_ids = c("mbz:", "contacts:", "all:", "total cases:"),
         
         stream_share = c(mbz_clean, contacts_clean, both_clean, "100"),
         
         N = c(nrow(mbz), nrow(contacts), nrow(both), nrow(x)))
  
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












