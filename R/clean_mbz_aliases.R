# Make aliases ------
str_normalize <- function(str){
  
  require(stringr)
  require(stringi)
  
  
  str <- str %>% 

    str_to_lower() %>%  

    stri_trans_general("Latin-ASCII") %>% # rm accents

    str_replace_all(c(
      #"\\bthe\\b" = "(the|les|des|du|de\sla)?", # LEAVE OUT FOR NOW
      "\\b(the|les|des|le|la)\\s\\b" = "", # remove the
      "\\b(?:and|et|&|n)\\b" = "&", # unify &
      "-" = " "
    )) %>% 
    
    str_remove_all("[.,!?;:]") %>% 
    
    str_squish() # trim + remove extra spaces

  return(str)
}


 make_aliases <- function(all_final, mbz_alias_file) {
  
  require(tidytable)
  require(dplyr)
  require(stringr)

  aliases <- load_s3(mbz_alias_file)
  
  ## complete cases only so cases with no match get NA
  ## for dz_stream_share --> the valid cases are prioritized
  ## before popularity is compared among them
  all_final <- all_final %>% 
    filter(!is.na(mbz_artist_id) & !is.na(sc_artist_id)) %>% 
    select(dz_artist_id, mbz_artist_id, dz_stream_share)
  
  aliases <- aliases %>% 
    rename(mbz_artist_id = "mbid",
           mbz_alias = "name") %>% 
    left_join(all_final, by = "mbz_artist_id") %>% 
    filter(!is.na(dz_artist_id)) %>% 
    arrange(desc(dz_stream_share)) %>% 
    select(dz_artist_id, mbz_alias, type, dz_stream_share) %>% 
    as_tibble()
  
  # remove useless names (hand-coded by sam)
  regex_fixes <- read_csv("data/regex_fixes.csv")
  names_to_remove <- regex_fixes %>% 
    filter(type == "remove")
  
  aliases <- aliases %>% 
    anti_join(names_to_remove, by = c(mbz_alias = "name"))
  
  
  # NORMALIZE NAMES!
  aliases <- aliases %>% 
    mutate(mbz_alias = str_normalize(mbz_alias)) 
  
  # remove duplicate names by known popularity method
  aliases <- aliases %>%
    group_by(mbz_alias) %>%
    filter(dz_stream_share == max(dz_stream_share)) %>% # IMPLEMENT RATIO
    add_count(mbz_alias) %>%
    ungroup()

  ### deduplicate remaining by name > alias
  remaining_dups <- aliases %>%
    filter(n > 1) %>%
    filter(type == "name") %>%
    select(-n)

  ### reinclude in aliases
  aliases <- aliases %>%
    select(-n) %>%
    anti_join(remaining_dups, by = "mbz_alias") %>%
    bind_rows(remaining_dups) %>%
    add_count(mbz_alias) %>%
    filter(n == 1) %>% # deletes one final rogue duplicate
    select(-n)
  
  # We clean up the regexes a bit
  aliases <- aliases %>% 
    filter(
      str_length(mbz_alias) > 1,# remove names of length 1
      !str_detect(mbz_alias, "^\'*[a-zA-Zéèê]{1,2}\'*$"), # remove names of two letters
      !str_detect(mbz_alias, "^\\d+$"),# and those of just numbers
      !str_detect(mbz_alias, "^[\u0621-\u064A]+$"), # those only in arabic
      # And those only in non-ascii characters (ie japanese, chinese, 
      # arabic, korean, russian, greek alphabets)
      !str_detect(mbz_alias, "^[^ -~]+$")
    ) %>% 
    
    distinct(dz_artist_id, mbz_alias, .keep_all = TRUE) 
    
  return(aliases)
}



 
 
 
 
 
 
 
 
 
 



  