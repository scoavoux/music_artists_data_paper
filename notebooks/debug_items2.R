library(purrr)
library(stringr)
library(dplyr)
library(tidyr)
library(arrow)


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



#### after separating feats: filtering songs with no deezer_id.old in deezer_id.new

## -----------------------
no_common_id <- items_old %>% 
  inner_join(items_new, # join items_old and items_new by song_id
             by = "song_id", 
             suffix = c(".old", ".new")) %>%
  group_by(song_id, deezer_id.old) %>%
  mutate(shared_deezer_id = any(deezer_id.old == deezer_id.new)) %>% # 
  group_by(song_id) %>%
  mutate(keep_song = any(!shared_deezer_id)) %>%
  ungroup() %>%
  filter(keep_song) %>% 
  select(song_id,
         song_title = "song_title.old",
         as.integer(deezer_id.old),
         as.integer(deezer_id.new)) %>% 
  arrange(song_id, song_title)
## -----------------------

write_parquet(no_common_id, "data/interim/no_common_id.parquet",
              compression = "snappy")


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
  left_join(names_old, by = "deezer_id.old") %>% # merge to old 
  left_join(names_new, by = "deezer_id.new") %>% 
  arrange(desc(f_n_play)) %>% 
  select(song_title, name_old_id, name_new_id,
         deezer_id.old, deezer_id.new,
         n_play, f_n_play, song_id)







