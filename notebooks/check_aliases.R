
options(scipen = 99)

tar_load(aliases)
tar_load(all_final)

# ------ join aliases to all_final
## complete cases only: cases with no match get 
## NA for dz_stream_share so the valid cases are prioritized
all_final <- all_final %>% 
  filter(!is.na(mbz_artist_id) & !is.na(sc_artist_id)) %>% 
  select(dz_artist_id, dz_stream_share)

aliases <- aliases %>% 
  left_join(all_final, by = "dz_artist_id")


## ------------- check homonyms and take the more popular artist
#### 20 ties, all but one are same name and alias with different id 
#### --> keep name > alias

aliases <- aliases %>% 
  group_by(mbz_alias) %>% 
  filter(dz_stream_share == max(dz_stream_share)) %>% 
  add_count(mbz_alias) %>% 
  ungroup()

# deduplicate remaining by name > alias
remaining_dups <- aliases %>% 
  filter(n > 1) %>% 
  filter(type == "name") %>% 
  select(-n)

aliases <- aliases %>% 
  select(-n) %>% 
  anti_join(remaining_dups, by = "mbz_alias") %>% 
  bind_rows(remaining_dups) %>% 
  add_count(mbz_alias) %>% 
  filter(n == 1) %>% # deletes one final rogue duplicate
  select(-n)



## ------------- check stopnames, esp:

#### first names only

#### common names (referring to something else, like "Paris")

#### very short names? maybe check separately to see if not too common


## -------------  delete The/Les etc? 
####because in text it could often be replaced
#### by du, des, de... e.g. "comme un air de Rolling Stones"
#### ask samuel



## ------------- append last names as alias to real names





















