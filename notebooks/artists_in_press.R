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
  select(c(entity, name_count)) %>% 
  distinct()  # remove perfect duplicates


############################################################
############################################################

# ------ 1. match on name in dz_names
press_v0 <- all_final %>% 
  left_join(ents, by = c(dz_name = "entity"))

# 2. ENTITIES WITH NO MATCH
ent_no_dzname <- ents %>% 
  anti_join(artists_in_press, by = c(entity = "dz_name")) %>% 
  filter(name_count > 30)

write.csv2(ent_no_dzname, file = "data/ent_no_dzname.csv")


## REIMPORT HAND-CODED AND MATCH WITH aliases
aliases_no_match <- read.csv("data/ent_no_dzname.csv", sep = ";") # UPDATED BY HAND!

aliases_no_match <- aliases_no_match %>% 
  filter(alias == 1 & to_artist != "") %>% 
  left_join(all_final, by = c(to_artist = "dz_name")) %>%
  rename(dz_name = to_artist,
         press_alias = entity) %>% 
  select(press_alias, dz_name, name_count) %>% 
  as_tibble()


## UPDATE ARTISTS_IN_PRESS WITH HAND-CODED ALIASES
press_v1 <- press_v0 %>%
  left_join(aliases_no_match, by = "dz_name", suffix = c("", "_alias")) %>%
  mutate(name_count = name_count + coalesce(name_count_alias, 0)) %>%
  select(-name_count_alias)


# ENTITIES WITH WRONG MATCH
# join and sort by difference between popularity and name count
# outliers: log ratio of the 2 metrics
press_v1_outliers <- press_v1 %>% 
  mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% 
  filter(name_count > 30) %>% 
  arrange(desc(corr_pop)) %>% 
  select(dz_name, name_count, keep_name, 
         dz_artist_id, corr_pop, dz_stream_share)
  
write.csv2(press_v1_outliers, file = "data/press_outliers.csv")


## REIMPORT CORRECTED OUTLIERS
press_v1_CORR_outliers <- read.csv("data/press_outliers.csv", sep = ";") # UPDATED BY HAND!

to_drop <- press_v1_CORR_outliers %>% 
  filter(drop == 1)

wrong_alias <- press_v1_CORR_outliers %>% 
  filter(alias == 1) %>% 
  rename(press_alias = dz_name,
         dz_name = to_artist) %>% 
  select(press_alias, dz_name, name_count) %>% 
  as_tibble()

# wrongly matched homonyms to integrate with name_count == 0
wrong_homonyms <- tibble(dz_name = wrong_alias$press_alias,
                   name_count = 0)
  
# UPDATE DF

### drop non-artists
press_v2 <- press_v1 %>% 
  anti_join(to_drop, by = "dz_name")

### recoded aliases
press_v3 <- press_v2 %>%
  left_join(wrong_alias, by = "dz_name", suffix = c("", "_alias")) %>%
  mutate(name_count = name_count + coalesce(name_count_alias, 0)) %>% # sum of counts if name in alias
  select(-c(name_count_alias, press_alias))


### homonyms
press_v4 <- press_v3 %>%
  mutate(name_count = ifelse(dz_name %in% wrong_homonyms$dz_name, 
                             0, 
                             name_count))

# FINAL
press_v4 <- press_v4 %>% 
  arrange(desc(name_count)) %>% 
  select(dz_name, name_count, dz_stream_share, dz_artist_id)

write.csv2(press_v4[1:500,], "data/mentions_in_press_0503.csv")

# BIND ALIASES LISTS TO EXPORT
found_aliases <- rbind(wrong_alias, aliases_no_match)
found_aliases <- found_aliases %>% 
  arrange(desc(name_count))

write.csv2(found_aliases, "data/handcoded_aliases_0503.csv")
























