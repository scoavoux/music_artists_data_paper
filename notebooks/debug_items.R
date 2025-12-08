library(purrr)
library(stringr)
library(dplyr)
library(tidyr)
library(arrow)
library(openxlsx)


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



#### after separating feats: filter songs with no deezer_id.old in deezer_id.new
#### i.e., songs which have one or more conflicted versions

## -----------------------
no_common_id <- items_old %>% 
  inner_join(items_new, 
             by = "song_id", 
             suffix = c(".old", ".new")) %>%
  group_by(song_id, deezer_id.old) %>%
  mutate(shared_deezer_id = any(deezer_id.old == deezer_id.new)) %>% # boolean col "any shared id"
  group_by(song_id) %>%
  mutate(keep_song = any(!shared_deezer_id)) %>% # boolean column "keep song"
  ungroup() %>%
  filter(keep_song)  %>% # drop if FALSE
  select(song_id,
         song_title = "song_title.old", # keep old only because song title never changes
         deezer_id.old = as.integer(deezer_id.old),
         deezer_id.new = as.integer(deezer_id.new)) %>% # to int for later operations
  arrange(song_id, song_title)



write_parquet(no_common_id, "data/interim/no_common_id.parquet",
              compression = "snappy")

items <- items_old %>% 
  inner_join(items_new, 
           by = "song_id", 
           suffix = c(".old", ".new"))

### join to streams and names ---------------------
conflicts <- no_common_id %>%
  inner_join(streams, by = "song_id") 

tar_load(names)

names_old <- names %>% 
  select(deezer_id.old = artist_id, 
         name_old_id = name)

names_new <- names %>% 
  select(deezer_id.new = artist_id, 
         name_new_id = name)

conflicts <- conflicts %>%
  left_join(names_old, by = "deezer_id.old") %>% 
  left_join(names_new, by = "deezer_id.new") %>% 
  arrange(desc(f_n_play)) %>% 
  select(song_title, 
         name_old_id, 
         name_new_id,
         deezer_id.old, 
         deezer_id.new,
         n_play, 
         f_n_play, 
         song_id)



#### THIS EVENING: SCRAPE ALL THESE NAMES IN ONE TARGET!
items_new_name_na <- items_new %>% 
  mutate(deezer_id = as.integer(deezer_id)) %>% 
  left_join(names, by = "deezer_id") %>% 
  distinct(deezer_id, .keep_all = TRUE)

sum(is.na(items_new_name_na$name))
  


### join scraped names ---------------------

conflicts <- conflicts %>%
  left_join(new_artists_names_from_api, by = "deezer_id.new") %>%
  mutate(name_new_id = coalesce(name_new_id, name),
         deezer_match = ifelse(deezer_id.old == deezer_id.new, TRUE, FALSE)) %>%
  select(-name)

nrow(conflicts)



### prepare matching ------------------------

removed <- conflicts %>% 
  group_by(song_id, deezer_id.old) %>%
  filter(any(deezer_id.old  == deezer_id.new)) %>% 
  ungroup() %>% 
  filter(deezer_id.old  == deezer_id.new) %>% 
  select(song_id, deezer_id.new)

to_match <- conflicts %>% 
  group_by(song_id, deezer_id.old) %>% 
  filter(!any(deezer_id.old  == deezer_id.new)) %>% 
  ungroup() %>% 
  anti_join(removed) %>% 
  distinct(deezer_id.old, deezer_id.new, .keep_all = TRUE) %>% 
  select(f_n_play, name_old_id, name_new_id, deezer_id.old, deezer_id.new)


write_csv2(to_match_2, "data/interim/conflicts_to_match.csv")

matched_scores <- read.xlsx("data/interim/matched_scores.xlsx")

matched_scores <- tibble(matched_scores)

matched_scores <- matched_scores %>% 
  left_join(pop_sum, by = c("deezer_id.old", "deezer_id.new"))


### see score of missing artists
unmatched <- matched_scores %>% 
  filter(is.na(match) | match != 1)

sum(unmatched$f_n_play) * 100









