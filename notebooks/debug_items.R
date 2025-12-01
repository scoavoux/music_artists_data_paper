## items sanity checks: issues with cases not matching between items_old and items_new

library(purrr)
library(dplyr)

tar_source("R")


## ------------------------------------------------------------------------------------
## load data
items_old <- load_s3("records_w3/items/songs.snappy.parquet",
                     col_select = c("song_id","artist_id","artists_ids"))

items_old <- items_old %>% 
  anti_join(to_remove) %>% 
  select(song_id,
         deezer_feat_ids = "artists_ids",
         deezer_id = "artist_id") %>% 
  filter(!is.na(deezer_id)) %>% 
  
  items_new <- load_s3("records_w3/items/song.snappy.parquet",
                       col_select = c("song_id","artist_id","artists_ids"))

items_new <- items_new %>% 
  anti_join(to_remove) %>% 
  select(song_id,
         deezer_feat_ids = "artists_ids",
         deezer_id = "artist_id") %>% 
  filter(!is.na(deezer_id)) %>% 

tar_load(items)



## ------------------------------------------------------------------------------------
## identify issue
items_unique_rows <- items %>%  # all song-artist pairs are unique
  distinct()

#items %>% # there are songs which map to multiple artists --- comp. intensive
  #group_by(song_id) %>%
  #summarize(n_deezer = n_distinct(deezer_id)) %>%
  #filter(n_deezer > 1) %>% 
  #nrow()

# no duplicate songs in items_old and items_new alone
items_old %>% distinct(song_id)
items_new %>% distinct(song_id)

#### join with songs where possible
songs <- songs %>% 
  select(song_id, song_title)
items_old <- items_old %>% left_join(songs, by = "song_id")


conflicts <- items_old %>% # but there are conflicts between items_old and items_new
  inner_join(items_new, 
             by = "song_id", 
             suffix = c(".old", ".new")) %>%
  filter(deezer_id.old != deezer_id.new)

conflicts %>% nrow() # 28k conflicted songs


### intersections between old and new feats: ≥ 1 artist of feats_old in feats_new?
conflicts <- conflicts %>%
  mutate(
    feat_old = map(deezer_feat_ids.old, as.vector),
    feat_new = map(deezer_feat_ids.new, as.vector)
  )

### create column to check if there is an overlap
### for descriptive purposes only!
conflicts %>%
  mutate(
    has_feat_overlap = map2_lgl(
      feat_old,
      feat_new,
      ~ length(intersect(.x, .y)) > 0
    )
  ) %>% count(has_feat_overlap) # approx. 21k w/o overlap, 7k w/ overlap


### popularity of affected artists/songs ---------------------
conflicts <- conflicts %>%
  inner_join(streams, by = "song_id")

# tar_load(names) # load deezer names

names_old <- names %>% 
  select(deezer_id.old = artist_id, 
         name_old_id = name)

names_new <- names %>% 
  select(deezer_id.new = artist_id, 
         name_new_id = name)

conflicts_named <- conflicts %>%
  left_join(names_old, by = "deezer_id.old") %>% 
  left_join(names_new, by = "deezer_id.new") %>% 
  arrange(desc(f_n_play)) %>% 
  select(song_title, name_old_id, name_new_id,
         deezer_id.old, deezer_feat_ids.old,
         deezer_id.new, deezer_feat_ids.new,
         n_play, f_n_play, song_id)

# interesting: many NAs in new names, no NAs in old names
sum(is.na(conflicts_named$name_old_id))
sum(is.na(conflicts_named$name_new_id))



#conflicts_named_1000 <- conflicts_named[1:1000,]
#conflicts_named_1000 <- conflicts_named_1000 %>%
#  mutate(
#    deezer_feat_ids.old = map_chr(deezer_feat_ids.old, ~ paste(as.integer(.x), collapse = ",")),
#    deezer_feat_ids.new = map_chr(deezer_feat_ids.new, ~ paste(as.integer(.x), collapse = ","))
#  )
#write.csv2(conflicts_named_1000, "data/interim/conflicted_ids.csv")
















