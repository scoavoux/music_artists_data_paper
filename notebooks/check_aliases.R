
options(scipen = 99)

tar_load(aliases)
tar_load(all_final)

install.packages('babynames')
library(babynames)

us_names <- babynames %>% 
  group_by(name) %>% 
  summarise(n = sum(n)) %>% 
  arrange(desc(n)) %>% 
  slice_head(n = 1000) %>% 
  select(name)

french_names <- read_csv("data/firstnames.csv")

french_names <- french_names %>% 
  slice_head(n = 10000) %>% 
  select(name = "firstname")

first_names <- french_names %>% 
  bind_rows(us_names) %>% 
  mutate(name = str_to_lower(name)) %>% 
  distinct()

first_names %>% 
  filter(name == "santana")

## ------------- check stopnames, esp:

#### first names only 
#### --- extract w first name dictionary

t <- aliases %>% 
  mutate(name = str_to_lower(mbz_alias)) %>% 
  inner_join(french_names, by = "name")







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





















