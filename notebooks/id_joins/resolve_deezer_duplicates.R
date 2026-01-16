# among the duplicate deezer names for which there is one single contact_name 
# and contact_id, find the cases where one duplicate is much more popular than the others
# try different thresholds

options(scipen = 99)

all <- load_s3("interim/consolidated_artists.csv")

tar_load(contacts)

cleanpop(all)


# for each name in all, compute fraction of streams held by one homonym
all_pop_share <- all %>% 
  group_by(name) %>% # maybe: name, deezer_id?
  mutate(pop_share = pop / sum(pop))

# filter the clear cases missing contact_ids
all_pop_share_co <- all_pop_share %>% 
  filter(pop_share > 0.90) %>% 
  filter(is.na(contact_id))

all_pop_share_co %>% filter(name == "Lomepal")

# ------------------ integrate to all --- try with my custom functions

## subset unique contact names
## because those are the ones we can clearly assign to a deezer artist
unique_contacts <- contacts %>% 
  select(-spotify_id) %>% 
  add_count(contact_name) %>% 
  filter(n == 1) %>% 
  select(-n)

unique_contacts %>% as_tibble %>% filter(contact_name == "Lomepal")

added_contacts <- unique_name_match(
  miss = all_pop_share_co,
  ref = unique_contacts,
  miss_name = "name",
  ref_name = "contact_name",
  id_col = "contact_id"
)

added_contacts %>% filter(contact_id == 1180989)


all <- left_join_coalesce(
  all,
  added_contacts,
  by = "deezer_id",
  col = c("contact_id")
)

all %>% filter(name == "Lomepal") %>% as_tibble()


# REPEAT FOR MBZ!
# filter the clear cases missing contact_ids
all_pop_share_mbz <- all_pop_share %>% 
  filter(pop_share > 0.90) %>% 
  filter(is.na(musicBrainzID))

unique_mbz <- mbz_deezer %>% 
  add_count(mbz_name) %>% 
  filter(n == 1) %>% 
  filter(is.na(deezerID)) %>% 
  select(-n)


added_mbz <- unique_name_match(
  miss = all_pop_share_mbz,
  ref = unique_mbz,
  miss_name = "name",
  ref_name = "mbz_name",
  id_col = "musicBrainzID"
)

cleanpop(all)

all <- left_join_coalesce(
  all,
  added_mbz,
  by = "deezer_id",
  col = c("musicBrainzID")
)


cleanpop(all)


write_s3(all, "interim/consolidated_artists_resolved_dups.csv")


















