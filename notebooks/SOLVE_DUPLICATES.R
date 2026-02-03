

wailers <- all_before_dedup %>% 
  filter(str_detect(name, "Wailers"))

tar_load(contacts)


all <- tibble(name = c("Jimi Hendrix", "The Jimi Hendrix Experience"), 
              deezer_id = c("1000", "1000"), 
              musicbrainz_id = c("tokeep", "todrop"), 
              contact_id = c("tokeep", "todrop"), 
              pop = c(0.1, 0.002), 
              collection_count = c(5200, 113))

all <- alldup[4:6,]
all


# -----------------------------------------------------------

coll_count <- contacts %>% 
  mutate(collection_count = as.integer(collection_count)) %>% 
  select(contact_id, collection_count)
    
all <- all %>% 
  left_join(coll_count, by = "contact_id")


all_dup <- all %>% 
  add_count(deezer_id) %>% 
  filter(n > 1) %>% # duplicates
  group_by(deezer_id) %>%
  mutate(score_share = collection_count / sum(collection_count))

sum(all$collection_count)


all_dup %>% 
  select(name, collection_count, score_share)


to_keep <- all_dup %>% 
  filter(score_share > 0.9) %>% 
  select(-c(score_share, collection_count, n))

to_keep

all <- all %>% 
  anti_join(to_keep, by = "deezer_id") %>% 
  bind_rows(to_keep)
  







