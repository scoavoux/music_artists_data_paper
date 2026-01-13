# among the duplicate deezer names for which there is one single contact_name 
# and contact_id, find the cases where one duplicate is much more popular than the others
# try different thresholds

options(scipen = 99)

cleanpop(all)


# for each name in all, compute fraction of streams held by one homonym
all_pop_share <- all %>% 
  group_by(name) %>% # maybe: name, deezer_id?
  mutate(pop_share = pop / sum(pop))

# filter the clear cases missing contact_ids
all_pop_share_co <- all_pop_share %>% 
  filter(pop_share > 0.90) %>% 
  filter(is.na(contact_id))

# ------------------ join to unique contact_names

## subset unique contact names
## because those are the ones we can clearly assign to a deezer artist
unique_contacts <- contacts %>% 
  select(-spotify_id) %>% 
  add_count(contact_name) %>% 
  filter(n == 1) %>% 
  select(-n)


added_contacts <- all_pop_share_co %>%
  inner_join(unique_contacts, by = c(name = "contact_name")) %>% 
  mutate(contact_id = contact_id.y) %>% 
  select(-c(contact_id.x, contact_id.y))

nrow(added_contacts)
sum(added_contacts$pop)

# REPEAT FOR MBZ!
# filter the clear cases missing contact_ids
all_pop_share_mbz <- all_pop_share %>% 
  filter(pop_share > 0.90) %>% 
  filter(is.na(musicBrainzID))

unique_mbz <- mbz_deezer %>% 
  add_count(mbz_name) %>% 
  filter(n == 1) %>% 
  select(-n)

added_mbz <- all_pop_share_mbz %>%
  inner_join(unique_mbz, by = c(name = "mbz_name")) %>% 
  mutate(musicBrainzID = musicBrainzID.y) %>% 
  select(-c(musicBrainzID.x, musicBrainzID.y))

nrow(added_mbz)
sum(added_mbz$pop)


# ----------- inspect missings

## contacts still missing after this step
miss_co <- all_pop_share_co %>% 
  anti_join(added_contacts, by = "deezer_id")

sum(miss_co$pop)
View(miss_co)


## mbz still missing after this step
miss_mbz <- all_pop_share_mbz %>% 
  anti_join(added_mbz, by = "deezer_id")

sum(miss_mbz$pop)
View(miss_mbz)



write.csv2(miss_co[1:200,], "data/missing_contacts_200.csv")
write.csv2(miss_mbz[1:200,], "data/missing_mbz_200.csv")


mbz_deezer %>% 
  filter(str_detect(mbz_name, "4Keus"))



















