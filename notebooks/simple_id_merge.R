library(dplyr)

alt <- tibble(song_title = c("i feel fine", 
                             "LA woman",
                             "subzero", 
                             "africa"),
              song_id = c(1,3,5,7),
              deezer_id = c("beatles", "doors", "ben klock", "toto"))


neu <- tibble(song_title = c("i feel fine", 
                             "LA woman",
                             "subzero", 
                             "africa"),
              song_id = c(1,3,5,7),
              deezer_id = c("beatles", "doors", "ben klock", "weezer"))


old <- items_old[1:10000,] %>% 
  arrange(desc(song_id)) %>% 
  rename(deezer_feat_id = "deezer_feat_ids")

new <- items_new[1:10000,] %>% 
  arrange(desc(song_id)) %>% 
  rename(deezer_feat_id = "deezer_feat_ids")


items <- items_new %>% 
  bind_rows(
    items_old %>% 
      anti_join(items_new, by = "song_id")
  )

items[which(is.na(items$deezer_id)),] # 3 NAs in deezer_id. drop for now
items <- na.omit(items)

items <- items %>% 
  mutate(deezer_feat_id = map_chr(deezer_feat_id, 
                                  ~ paste(as.integer(.x), 
                                          collapse = ","))) %>% 
  filter(!is.na(deezer_id)) %>% 
  separate_rows(deezer_feat_id, sep = ",") %>% 
  select(song_id,
         song_title,
         deezer_id,
         deezer_feat_id)

## weight by n featured artists
items <- items %>%
  group_by(song_id) %>%
  mutate(w_feat = 1 / n_distinct(deezer_feat_id))



## join to streams



i <- items %>% 
  inner_join(streams, by = "song_id")

i <- i %>% 
  mutate(weighted_f_n_play = w_feat * f_n_play)












