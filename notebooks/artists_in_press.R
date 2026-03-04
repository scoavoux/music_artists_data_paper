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

df <- read.csv("data/ent_no_dzname.csv", sep = ";")
df <- df %>% 
  filter(alias == 1 & to_artist != "") 

df <- df %>% 
  left_join(all_final, by = c(to_artist = "dz_name")) %>%
  rename(dz_name = to_artist) %>% 
  select(dz_name, name_count)



# ENTITIES WITH WRONG MATCH
# join and sort by difference between popularity and name count
artists_in_press <- artists_in_press %>% 
  mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% 
  arrange(desc(corr_pop)) # outliers: log ratio of the 2 metrics


all_final %>% 
  filter(str_detect(dz_name, "kavinsk"))


all_final %>% 
  filter(dz_artist_id == 1004)

all_before_dedup %>% 
  filter(name == "N.W.A")









# # what additional matches do we get from the aliases list?
# added_aliases <- aliases_in_press %>% 
#   anti_join(artists_in_press, by = c(mbz_alias = "dz_name")) %>% 
#   arrange(desc(name_count)) %>% 
#   select(-c(type))

write.csv2(added_aliases, file = "data/aliases_to_check.csv")

dz_matches_outliers <- artists_in_press %>% 
  select(dz_artist_id, dz_name, dz_stream_share, name_count, corr_pop)

write.csv2(dz_matches_outliers[1:1000,], file = "data/dz_matches_outliers.csv")

artists_in_press <- artists_in_press %>% 
  arrange(desc(name_count))
















