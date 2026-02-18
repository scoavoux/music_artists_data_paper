# Make aliases ------

regexify <- function(str){
  
  require(stringr)
  
  str <- str %>% 
    #stringi::stri_trans_general(id = "Latin-ASCII") %>% 
    # escape regex special character
    str_escape() %>% 
    
    str_replace_all(c(
      "é" = "[ée]",
      "\\b[Tt]he\\b" = "([Tt]he|[Ll]es)",
      "^[Tt]he\\b" = "([Tt]he|[Ll]es)",
      "\\b(?:[Aa]nd|[Ee]t|&)" = "([Aa]nd|[Ee]t|&)"
    )) %>% 
  
  { ifelse(str_detect(substr(., 1, 1), "\\w"), paste0("\\b", .), .) } %>%
  { ifelse(str_detect(substr(., nchar(.), nchar(.)), "\\w"), paste0(., "\\b"), .) }
    
  return(str)
}


make_aliases <- function(all_final, mbz_alias_file) {
  
  require(tidytable)
  require(dplyr)
  require(stringr)
  
  aliases <- load_s3(mbz_alias_file)
  
  aliases <- aliases %>% 
    rename(mbz_artist_id = "mbid",
           mbz_alias = "name") %>% 
    left_join(all_final, by = "mbz_artist_id") %>% 
    filter(!is.na(dz_artist_id)) %>% 
    arrange(desc(dz_stream_share)) %>% 
    mutate(alias_regex = regexify(mbz_alias)) %>% 
    select(dz_artist_id, mbz_alias, alias_regex, type) %>% 
    as_tibble()

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
    
}

  

### ADD LAST NAMES AS ALIAS HERE?


  