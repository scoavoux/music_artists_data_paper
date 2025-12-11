library(dplyr)


old <- items_old[1:10000,] %>% 
  arrange(desc(song_id))

new <- items_new[1:10000,] %>% 
  arrange(desc(song_id))

old %>% 
  distinct(song_id)


new %>% 
  anti_join(old, by = "song_id")


items <- new %>% 
  bind_rows(
  old %>% 
    anti_join(items_new, by = "song_id")
)











