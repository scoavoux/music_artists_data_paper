# among the duplicate names, find the cases where one duplicate is
# much more popular than the others
# try different thresholds

options(scipen = 99)

## resolve deezer_id duplicate conflicts first!!
## --> need to identify the cases for which this applies at all

## multiple contact_id or musicBrainzID, same name, different popularity
## FORGET mbz and contacts FOR NOW


## first: same name, different deezer_id


## subset duplicate deezer names

### all duplicate names
dup <- all %>% 
  count(name) %>% 
  filter(n > 1) %>% 
  arrange(desc(n))

### 
dup_in_all <- all %>% 
  distinct(deezer_id, pop, .keep_all = T) %>% 
  inner_join(dup, by = "name")

# for each duplicate name, compute fraction of streams held by one homonym
test <- dup_in_all %>% 
  group_by(name) %>% 
  mutate(leader = max(pop) / sum(pop)) %>% 
  ungroup()




all %>% 
  filter(name %in% c("Dadju", "PLK", "Kendji Girac")) ## what? confusion between 3 artists

## comes from feats --- identify and solve this!!
## maybe add names later only??


artists %>% 
  filter(name == "Dadju")


names <- load_s3("records_w3/items/artists_data.snappy.parquet")

names %>% 
  filter(name == "Kendji Girac")





