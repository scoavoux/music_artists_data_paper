# -------------------------------------------------------------------

# careful: because the id pairs matter, not the song_id

# select only relevant info from match
match <- matched_names %>% 
  select(deezer_id_old, 
         deezer_id_new, 
         match)

# full join items_old and items_new
items <- items_old %>% 
  full_join(items_new, by = c("song_id")) %>% 
  select(-c(song_title.x, song_title.y)) %>% 
  rename(deezer_id_old = "deezer_id.x",
         deezer_id_new = "deezer_id.y") %>% 
  mutate(deezer_id_new = as.integer(deezer_id_new),
         deezer_id_old = as.integer(deezer_id_old))

it <- items[1:100000,]

# join with match
it_match <- items %>% 
  left_join(match, by = c("deezer_id_old", "deezer_id_new")) %>% 
  filter(match == 1) 

items %>% 
  inner_join(match, by = c("deezer_id_old", "deezer_id_new")) %>% 
  filter(match == 1) 


it_match %>%
  group_by(song_id) %>% 
  filter(any(match == 1))


matched_names %>% 
  filter(str_detect(name_old_id, "[&\\/\\\\]")) 


View(matched_names)
























