library(dplyr)
library(stringr)
library(ggplot2)

tar_load(aliases)
tar_load(all_final)

all_final %>% 
  filter(dz_artist_id == 1203)

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
ents <- read.csv("data/extracted_ents_clean.csv", sep = ";")

ents <- ents %>% 
  as_tibble() %>% 
  mutate(is_in_press = TRUE,
         press_name = normalize_string(name)) %>% 
  group_by(press_name) %>% 
  mutate(keep_name = ifelse(name_count == max(name_count), TRUE, FALSE)) %>% 
  filter(keep_name) %>% 
  ungroup() %>% 
  select(c(press_name, name_count, is_in_press, keep_name)) %>% 
  distinct()  # remove perfect duplicates


# ------ compare join with dz_names and with aliases
# join and sort by difference between popularity and name count
artists_in_press <- all_final %>% 
  left_join(ents, by = c(dz_name = "press_name")) %>% 
  mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% # outliers: log ratio of the 2 metrics
  arrange(desc(corr_pop))

aliases_in_press <- aliases %>% 
  inner_join(ents, by = c(mbz_alias = "press_name")) 

# what additional matches do we get from the aliases list?
added_aliases <- aliases_in_press %>% 
  anti_join(artists_in_press, by = c(mbz_alias = "dz_name")) %>% 
  arrange(desc(name_count)) %>% 
  select(-c(keep_name, is_in_press, type))


write.csv2(added_aliases, file = "data/aliases_to_check.csv")


dz_matches_outliers <- artists_in_press %>% 
  select(dz_artist_id, dz_name, dz_stream_share, name_count, corr_pop)

write.csv2(dz_matches_outliers[1:1000,], file = "data/dz_matches_outliers.csv")

artists_in_press <- artists_in_press %>% 
  arrange(desc(name_count))
















