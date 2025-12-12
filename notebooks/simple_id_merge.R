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

## weight by n featured artists and join to streams
items <- items %>%
  inner_join(streams, by = "song_id") %>% 
  group_by(song_id) %>%
  mutate(w_feat = 1 / n_distinct(deezer_feat_id),
         weighted_f_n_play = w_feat * f_n_play)


# share of plays covered by items
sum(items$f_n_play * items$w_feat)


items %>%
  group_by(song_id) %>%
  summarise(f_n_play = sum(n_play) / sum(total_plays))


unweight <- items %>% 
  arrange(desc(f_n_play)) %>% 
  select(f_n_play)

unweight


weight <- items %>% 
  arrange(desc(weighted_f_n_play)) %>% 
  select(weighted_f_n_play)

weight

cor.test(unweight$f_n_play, 
         weight$weighted_f_n_play, 
         method = 'spearman')


items %>% 
  filter(w_feat < 1) %>% 
  distinct(song_id, .keep_all = T)

streams %>% 
  distinct(song_id, .keep_all = T)



old <- items_old %>% 
  inner_join(streams, by = "song_id")

sum(old$f_n_play)


new <- items_new %>% 
  inner_join(streams, by = "song_id")

both <- items %>% 
  inner_join(streams, by = "song_id") %>% 
  mutate(w_feat = 1 / n_distinct(deezer_feat_id))

both





df <- load_s3("records_w3/items/songs.snappy.parquet",
              col_select = c("song_id",
                             "artist_id",
                             "artists_ids",
                             "song_title")) %>% 

  select(song_id,
         song_title,
         deezer_feat_id = "artists_ids",
         deezer_id = "artist_id")


df2 <- df %>% 
  mutate(deezer_feat_id = map_chr(deezer_feat_id, 
                                  ~ paste(as.integer(.x), 
                                          collapse = ","))) %>% 
  filter(!is.na(deezer_id)) %>% 
  separate_rows(deezer_feat_id, sep = ",") %>% 
  select(song_id,
         song_title,
         deezer_id,
         deezer_feat_id)

df3 <- df2 %>%
  group_by(song_id) %>% 
  mutate(w_feat = 1 / n_distinct(deezer_feat_id)) %>% 
  inner_join(streams, by = "song_id") %>% 
  mutate(weighted_f_n_play = w_feat * f_n_play)



options(scipen = 99)
df
sum(df$weighted_f_n_play)






















