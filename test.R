


require(dplyr)
require(logging)

ref_id   <- "contact_id"
ref_name <- "contact_name"
all_name <- "name"
all_id   <- "contact_id"

# for each name in all, compute fraction of streams held by one homonym
# and filter the clear cases missing contact_ids
all_pop_share <- all %>% 
  group_by(name) %>% # maybe: name, deezer_id?
  mutate(pop_share = pop / sum(pop)) %>% 
  filter(pop_share > 0.90)  %>% 
  filter(is.na(contact_id))


## subset unique ref names
unique_ref <- contacts %>% 
  add_count(contact_name) %>% 
  filter(n == 1) %>% # unique names only
  select(contact_id, contact_name)


matches <- patch_names(all = all_pop_share,
            ref = unique_ref,
            ref_id = "contact_id",
            ref_name = "contact_name",
            all_name = "name",
            all_id = "contact_id")






