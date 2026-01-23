library(purrr)
library(stringr)
library(dplyr)
library(tidyr)
library(arrow)
library(openxlsx)
library(tidyverse)
library(tidyr)


### explode rows with feats
items_old <- load_s3("records_w3/items/songs.snappy.parquet",
                     col_select = c("song_id",
                                    "artist_id",
                                    "artists_ids",
                                    "song_title")) %>% 
  anti_join(to_remove_file, by = "artist_id") %>% 
  mutate(deezer_feat_id = map_chr(artists_ids, 
                                  ~ paste(as.integer(.x), 
                                          collapse = ","))) %>% 
  filter(!is.na(artist_id)) %>% 
  separate_rows(deezer_feat_id, sep = ",") %>% 
  select(song_id,
         song_title,
         deezer_id = "deezer_feat_id")

write_parquet(items_old, "data/interim/items_old_sep.parquet",
              compression = "snappy")


items_new <- load_s3("records_w3/items/song.snappy.parquet",
                     col_select = c("song_id",
                                    "artist_id",
                                    "artists_ids",
                                    "song_title")) %>% 
  anti_join(to_remove_file, by = "artist_id") %>% 
  mutate(deezer_feat_id = map_chr(artists_ids, 
                                   ~ paste(as.integer(.x), 
                                           collapse = ","))) %>% 
  filter(!is.na(artist_id)) %>% 
  separate_rows(deezer_feat_id, sep = ",") %>% 
  select(song_id,
         song_title,
         deezer_id = "deezer_feat_id")

write_parquet(items_new, "data/interim/items_new_sep.parquet",
              compression = "snappy")



#### after separating feats: filter songs with no deezer_id_old in deezer_id_new
#### i.e., songs which have one or more conflicted versions

## -----------------------
no_common_id <- items_old %>% 
  inner_join(items_new, 
             by = "song_id", 
             suffix = c("_old", "_new")) %>%
  group_by(song_id, deezer_id_old) %>%
  mutate(shared_deezer_id = any(deezer_id_old == deezer_id_new)) %>% # boolean col "any shared id"
  group_by(song_id) %>%
  mutate(keep_song = any(!shared_deezer_id)) %>% # boolean column "keep song"
  ungroup() %>%
  filter(keep_song)  %>% # drop if FALSE
  select(song_id,
         song_title = "song_title_old", # keep old only because song title never changes
         deezer_id_old = as.integer(deezer_id_old),
         deezer_id_new = as.integer(deezer_id_new)) %>% # to int for later operations
  arrange(song_id, song_title)



write_parquet(no_common_id, "data/interim/no_common_id.parquet",
              compression = "snappy")

items <- items_old %>% 
  inner_join(items_new, 
           by = "song_id", 
           suffix = c("_old", "_new"))

### join to streams and names ---------------------
conflicts <- no_common_id %>%
  inner_join(streams, by = "song_id") 

tar_load(names)

names_old <- names %>% 
  select(deezer_id_old = artist_id, 
         name_old_id = name)

names_new <- names %>% 
  select(deezer_id_new = artist_id, 
         name_new_id = name)

conflicts <- conflicts %>%
  left_join(names_old, by = "deezer_id_old") %>% 
  left_join(names_new, by = "deezer_id_new") %>% 
  arrange(desc(f_n_play)) %>% 
  select(song_title, 
         name_old_id, 
         name_new_id,
         deezer_id_old, 
         deezer_id_new,
         n_play, 
         f_n_play, 
         song_id)



#### TUESDAY EVENING: SCRAPE ALL THESE NAMES IN ONE TARGET!
items_new_name_na <- items_new %>% 
  mutate(deezer_id = as.integer(deezer_id)) %>% 
  left_join(names, by = "deezer_id") %>% 
  distinct(deezer_id, .keep_all = TRUE)

sum(is.na(items_new_name_na$name))
  


### join scraped names ---------------------

conflicts <- conflicts %>%
  left_join(new_artists_names_from_api, by = "deezer_id_new") %>%
  mutate(name_new_id = coalesce(name_new_id, name),
         deezer_match = ifelse(deezer_id_old == deezer_id_new, TRUE, FALSE)) %>%
  select(-name)

nrow(conflicts)



### prepare matching ------------------------

removed <- conflicts %>% 
  group_by(song_id, deezer_id_old) %>%
  filter(any(deezer_id_old == deezer_id_new)) %>% 
  ungroup() %>% 
  filter(deezer_id_old == deezer_id_new) %>% 
  select(song_id, deezer_id_new)

to_match <- conflicts %>% 
  group_by(song_id, deezer_id_old) %>% 
  filter(!any(deezer_id_old == deezer_id_new)) %>% 
  ungroup() %>% 
  anti_join(removed) %>% 
  distinct(deezer_id_old, deezer_id_new, .keep_all = TRUE) %>% 
  select(f_n_play, name_old_id, name_new_id, deezer_id_old, deezer_id_new)


write_csv2(to_match_2, "data/interim/conflicts_to_match.csv")




# -------------------------------------------------------------------

## BUILD CONSOLIDATED ITEMS

#### 1. rejoin to conflicts 

##### to conflicts_names: because I have hand-coded unique id pairs,
##### not unique songs! so join with conflicts first 
##### in order to expand matches to all affected song_ids

match <- matched_names %>% 
  select(deezer_id_old, 
         deezer_id_new, 
         match)

# apply match column to all conflicted songs
conflicts_matched <- conflicts_names %>%
  select(-c(X, ...1, deezer_match)) %>% 
  group_by(deezer_id_old, deezer_id_new) %>% 
  left_join(mat, by = c("deezer_id_old", "deezer_id_new")) %>% 
  ungroup() %>%
    select(song_id, deezer_id_old, deezer_id_new, same_artist)
  
items <- items_old %>% 
  full_join(items_new, by = c("song_id")) %>% 
  select(-c(song_title.x, song_title.y)) %>% 
  rename(deezer_id_old = "deezer_id.x",
         deezer_id_new = "deezer_id.y") %>% 
  mutate(deezer_id_new = as.integer(deezer_id_new),
         deezer_id_old = as.integer(deezer_id_old))

items <- items[1:100000,]


items_match <- items %>% 
  group_by(deezer_id_old, deezer_id_new) %>% 
  left_join(match, by = c("deezer_id_old", "deezer_id_new")) %>% 
  ungroup()
  























