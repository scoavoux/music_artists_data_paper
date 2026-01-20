# some contact_names cannot be safely linked to a deezer name because there are duplicates
# contact_ids. check nr of reviews etc to see if some of them are way more popular
# same logic as identify_deezer_duplicates.R!

tar_load(all)
tar_load(contacts)

# for each name in contacts, compute popularity of occurrences
## col_share: share of collection_counts
## prod_share: share of songs (?)
## gen_like_share: share of likes
contacts <- as_tibble(contacts) %>% 
  select(-c(contact_name_url, subtype_id, spotify_id,
            mbz_id, freebase_id)) %>% 
  # remove cases with no 
  filter(product_count > 0 & gen_like_count > 0 & collection_count > 0) %>% 
  group_by(contact_name) %>% 
  mutate(col_share = collection_count / sum(collection_count),
         prod_share = product_count / sum(product_count),
         gen_like_share = gen_like_count / sum(gen_like_count))


# ------- join to missing contact_data in all

# keep this condition for now: adding the other variables adds like 30 cases
# but unsure about the cases (e.g., there are weird ones with very few albums)
co_unique <- contacts %>% 
  filter(col_share > 0.9) %>% 
  select(contact_name, contact_id)

# important: subset all to unique names missing in contacts
all_unique_co <- all %>% 
  add_count(name) %>% 
  filter(n == 1) %>% 
  filter(is.na(contact_id))

added_contacts <- unique_name_match(
  miss = all_unique_co,
  ref = co_unique,
  miss_name = "name",
  ref_name = "contact_name",
  id_col = "contact_id"
)

all <- left_join_coalesce(
  all,
  added_contacts,
  by = "deezer_id",
  col = "contact_id"
)

cleanpop(all)


## anti_join added contacts with missing in all to subset cases still missing
t <- all_unique_co %>% 
  anti_join(added_contacts, by = "deezer_id")





















