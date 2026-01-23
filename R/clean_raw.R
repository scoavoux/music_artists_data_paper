## insert custom functions to clean the raw files here

## select relevant columns, filter duplicates,
## remove unused cases, recode ids to str, recode wrong ids (https...), etc.




contacts



## contacts
clean_contacts <- function(file){
  
  contacts <- load_s3(file)
  
  clean_contacts <- contacts %>% 
    
    # set "" names as NA
    mutate(
      mbz_id = na_if(mbz_id, ""),
      contact_name = na_if(contact_name, "")
    )
  
    # id cols to character for clean joins
    mutate_if(is.integer, as.character) %>%
    
    # clean (2) dirty ids
    mutate(mbz_id = str_remove(mbz_id, "https://musicbrainz.org/artist/"))  

    rename(musicbrainz_id = "mbz_id")
      
  select(contact_id, contact_name, collection_count, musicbrainz_id)
  as_tibble()
  
}

## mbz_deezer


## manual_search


## wiki