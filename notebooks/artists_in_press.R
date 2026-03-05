library(dplyr)
library(stringr)
library(ggplot2)

options(scipen = 99)

tar_load(all_final)

# prepare artists
all_final <- all_final %>% 
  filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% 
  mutate(dz_name = str_normalize(dz_name)) %>% 
  group_by(dz_name) %>% 
  mutate(keep = ifelse(dz_stream_share == max(dz_stream_share), TRUE, FALSE)) %>% 
  filter(keep == TRUE) %>% 
  ungroup() %>% 
  select(dz_name, dz_stream_share, dz_artist_id)


# prepare entities
ents <- read.csv("data/extracted_ents_ALL.csv", sep = ";")

ents <- ents %>% 
  as_tibble() %>% 
  mutate(is_in_press = TRUE,
         entity = str_normalize(name)) %>% 
  group_by(entity) %>% 
  mutate(keep_name = ifelse(name_count == max(name_count), TRUE, FALSE)) %>% 
  filter(keep_name) %>% 
  ungroup() %>% 
  filter(str_length(entity) > 2) %>% 
  select(c(entity, name_count, keep_name)) %>% 
  distinct()  # remove perfect duplicates


############################################################
############################################################

# ------ 1. match on name in dz_names
artists_in_press <- all_final %>% 
  left_join(ents, by = c(dz_name = "entity"))

# 2. ENTITIES WITH NO MATCH
ent_no_dzname <- ents %>% 
  anti_join(artists_in_press, by = c(entity = "dz_name")) %>% 
  filter(name_count > 30)

write.csv2(ent_no_dzname, file = "data/ent_no_dzname.csv")



## REIMPORT HAND-CODED AND MATCH WITH aliases

df <- read.csv("data/ent_no_dzname.csv", sep = ";") # UPDATED BY HAND!

df <- df %>% 
  filter(alias == 1 & to_artist != "") %>% 
  left_join(all_final, by = c(to_artist = "dz_name")) %>%
  rename(dz_name = to_artist) %>% 
  select(dz_name, name_count)


## UPDATE ARTISTS_IN_PRESS WITH HAND-CODED ALIASES
## ALREADY HERE?? COULD ALSO BIND THEM WITH THE OTHER
## FOUND ALIASES
press_update1 <- artists_in_press %>%
  left_join(
    df %>% summarise(name_count = sum(name_count), .by = "dz_name"),
    by = "dz_name",
    suffix = c("", "_add")
  ) %>%
  mutate(name_count = name_count + coalesce(name_count_add, 0)) %>%
  select(-name_count_add)


# ENTITIES WITH WRONG MATCH
# join and sort by difference between popularity and name count
# outliers: log ratio of the 2 metrics
press_outliers <- press_update1 %>% 
  mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% 
  filter(name_count > 30) %>% 
  arrange(desc(corr_pop)) %>% 
  select(dz_name, name_count, keep_name, 
         dz_artist_id, corr_pop, dz_stream_share)
  
write.csv2(press_outliers, file = "data/press_outliers.csv")


## REIMPORT CORRECTED OUTLIERS
df <- read.csv("data/press_outliers.csv", sep = ";") # UPDATED BY HAND!


to_drop <- df %>% 
  filter(drop == 1)

alias_to_name <- df %>% 
  filter(alias == 1) %>% 
  rename(press_alias = dz_name,
         dz_name = to_artist) %>% 
  select(press_alias, dz_name, name_count) %>% 
  as_tibble()



# homonyms to integrate
homonyms <- tibble(dz_name = alias_to_name$press_alias,
                   name_count = 0)
  

# UPDATE DF

### drop non-artists
press_update2 <- press_update1 %>% 
  anti_join(to_drop, by = "dz_name")

### recoded aliases
press_update2 <- press_update2 %>%
  left_join(
    alias_to_name %>% summarise(name_count = sum(name_count), .by = "dz_name"),
    by = "dz_name",
    suffix = c("", "_add")
  ) %>%
  mutate(name_count = name_count + coalesce(name_count_add, 0)) %>%
  select(-name_count_add)

### homonyms
press_update2 %>% 
  filter(dz_name == "dylan")

press_update2 <- press_update2 %>%
  mutate(name_count = ifelse(dz_name %in% homonyms$dz_name, 0, name_count))

# FINAL
press_update2 <- press_update2 %>% 
  arrange(desc(name_count))


##




























