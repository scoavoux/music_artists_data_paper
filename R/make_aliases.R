make_aliases <- function(senscritique_mb_deezer_id, 
                         artists_pop){
  require(tidyverse)
  require(tidytable)
  require(arrow)
  
  s3 <- initialize_s3()
  
  ## Names from deezer
  s3$download_file(Bucket = "scoavoux", 
                   Key = "records_w3/items/artists_data.snappy.parquet",
                   Filename = "data/temp/artists_data.snappy.parquet")
  
  artists <- read_parquet("data/temp/artists_data.snappy.parquet", 
                          col_select = 1:2)
  
  artists <- mutate(artists, 
                    type = "deezername")
  
  artists <- artists %>% 
    left_join(senscritique_mb_deezer_id) %>% 
    left_join(select(artists_pop, artist_id, control_n_users, respondent_n_users)) %>% 
    filter(!is.na(control_n_users) & is.na(respondent_n_users)) %>% 
    mutate(consolidated_artist_id = ifelse(is.na(consolidated_artist_id), artist_id, consolidated_artist_id)) %>% 
    select(consolidated_artist_id, name, type)
  
  ## Names and aliases from musicbrainz
  f <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_name_alias.csv")
  mb_aliases <- f$Body %>% rawToChar() %>% read_csv()
  rm(f)
  
  mb_aliases <- left_join(mb_aliases, senscritique_mb_deezer_id) %>% 
    filter(!is.na(consolidated_artist_id)) %>% 
    select(consolidated_artist_id, name, type) %>% 
    arrange(desc(type))
  
  consolidated_id_name_alias <- bind_rows(artists, mb_aliases) %>% 
    # separate names where several artists ;
    tidyr::separate_longer_delim(name, delim = ";") %>% 
    mutate(name = str_trim(name)) %>% 
    distinct(consolidated_artist_id, name, .keep_all = TRUE) %>% 
    rename(artist_id = consolidated_artist_id)
  regexify <- function(str){
    str %>% 
      #stringi::stri_trans_general(id = "Latin-ASCII") %>% 
      # escape regex special character
      str_replace_all(fixed("\\"), "\\\\") %>%
      str_replace_all(fixed("."), "\\.") %>%
      str_replace_all(fixed("+"), "\\+") %>%
      str_replace_all(fixed("*"), "\\*") %>%
      str_replace_all(fixed("?"), "\\?") %>%
      str_replace_all(fixed("^"), "\\^") %>%
      str_replace_all(fixed("$"), "\\$") %>%
      str_replace_all(fixed("("), "\\(") %>%
      str_replace_all(fixed(")"), "\\)") %>%
      str_replace_all(fixed("["), "\\[") %>%
      str_replace_all(fixed("]"), "\\]") %>%
      str_replace_all(fixed("{"), "\\{") %>%
      str_replace_all(fixed("}"), "\\}") %>%
      str_replace_all(fixed("|"), "\\|") %>%
      str_replace_all(fixed("-"), "\\-") %>%
      str_replace_all(fixed("\'"), ".") %>% 
      ifelse(str_detect(substr(., 1, 1), "\\w"), paste0("\\b", .), .) %>% 
      ifelse(str_detect(substr(., nchar(.), nchar(.)), "\\w"), paste0(., "\\b"), .) %>% 
      str_replace_all(c("é" = "[ée]",
                        "\\b[Tt]he\\b" = "([Tt]he|[Ll]es)",
                        "^[Tt]he\\b" = "([Tt]he|[Ll]es)",
                        "\\b[Aa]nd\\b|\\b[Ee]t\\b|&" = "([Aa]nd|[Ee]t|&)"))
  }
  
  artist_names_and_aliases <- consolidated_id_name_alias %>% 
    mutate(regex = regexify(name))
  return(artist_names_and_aliases)
}