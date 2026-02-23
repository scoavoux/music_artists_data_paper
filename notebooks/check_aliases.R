
options(scipen = 99)

tar_load(aliases)
tar_load(all_final)

library(babynames)
library(dplyr)
library(readr)
library(stringr)


## MUTATE A NAME COLUMN IN ALIASES


first_names <- first_name_dict(fr_names = "data/firstnames.csv",
                               n_us_names=500, n_fr_names=2000)

## ------------- check stopnames, esp:

#### first names only 
#### --- extract w first name dictionary

first_name_in_aliases <- aliases %>% 
  mutate(name = str_to_lower(mbz_alias)) %>% 
  inner_join(first_names, by = "name")

pattern <- str_c(paste0("\\b", first_names$name, "\\b"), collapse = "|")

string <- "Paul Simon"

str_count(string, pattern = '\\w+')


aliases <- aliases %>%
  mutate(
    mbz_alias = str_to_lower(mbz_alias),
    real_name = str_detect(mbz_alias, regex(pattern, ignore_case = TRUE))
  )


test <- t %>% 
  filter(real_name == TRUE) %>% 
  mutate(n_tokens = str_count(mbz_alias, pattern = '\\w+')) %>% 
  filter(n_tokens %in% c(2,3))




#### append last names as alias
#### --- extract real names w first name dictionary



#### common names (referring to something else, like "Paris")
#### sam hand-coded many already

#### very short names? maybe check separately to see if not too common


## -------------  delete The/Les etc? 
####because in text it could often be replaced
#### by du, des, de... e.g. "comme un air de Rolling Stones"
#### ask samuel


## ------------- append last names as alias to real names



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



















