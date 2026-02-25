require(tidytable)
require(dplyr)
require(stringr)


## MATCH ALIASES W NAMES!

dz_names <- all_final %>% 
  filter(!is.na(mbz_artist_id) & !is.na(sc_artist_id)) %>% 
  select(dz_artist_id, 
         mbz_alias = "dz_name", 
         dz_stream_share) %>% 
  mutate(type = "name")



# remove useless names (hand-coded by sam)
regex_fixes <- read_csv("data/regex_fixes.csv")
names_to_remove <- regex_fixes %>% 
  filter(type == "remove")

dz_names <- dz_names %>% 
  anti_join(names_to_remove, by = c(mbz_alias = "name"))


# We clean up the regexes a bit
dz_names <- dz_names %>% 
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



write.xlsx(dz_names, "data/dz_names.xlsx")


dz_names %>% 
  filter(str_detect(mbz_alias, "Mauvais"))

aliases %>% 
  filter(str_detect(mbz_alias, "homme"))






