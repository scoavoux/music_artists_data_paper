# among the duplicate deezer names for which there is one single contact_name 
# and contact_id, find the cases where one duplicate is much more popular than the others
# try different thresholds



patch_deezer_dups <- function(all, ref, ref_id, ref_names){
  
  ref_id   <- rlang::ensym(ref_id)
  ref_name <- rlang::ensym(ref_name)
  all_name <- rlang::ensym(all_name)
  all_id   <- rlang::ensym(all_id)
  
  # for each name in all, compute fraction of streams held by one homonym
  all_pop_share <- all %>% 
    group_by(name) %>% # maybe: name, deezer_id?
    mutate(pop_share = pop / sum(pop))
  
  # filter the clear cases missing contact_ids
  all_pop_share <- all_pop_share %>% 
    filter(pop_share > 0.90) %>% 
    filter(is.na(!!ref_id))
  
  ## subset unique ref names
  unique_ref <- ref %>% 
    #select(-spotify_id) %>% 
    add_count(!!ref_name) %>% 
    filter(n == 1) %>% # unique names only
    select(-n)
  
  
  t <- patch_names(all = all_pop_share,
              ref = unique_ref,
              ref_id = !!ref_id,
              ref_name = !!ref_name,
              all_name = name,
              all_id = !!ref_id)

  
  
}




# 
# # ------------------ integrate to all --- try with my custom functions
# 
# added_contacts <- unique_name_match(
#   miss = all_pop_share_co,
#   ref = unique_contacts,
#   miss_name = "name",
#   ref_name = "contact_name",
#   id_col = "contact_id"
# )
# 
# all <- left_join_coalesce(
#   all,
#   added_contacts,
#   by = "deezer_id",
#   col = c("contact_id")
# )
# 
# 
# # REPEAT FOR MBZ!
# # filter the clear cases missing contact_ids
# all_pop_share_mbz <- all_pop_share %>% 
#   filter(pop_share > 0.90) %>% 
#   filter(is.na(musicBrainzID))
# 
# unique_mbz <- mbz_deezer %>% 
#   add_count(mbz_name) %>% 
#   filter(n == 1) %>% 
#   filter(is.na(deezerID)) %>% 
#   select(-n)
# 
# 
# added_mbz <- unique_name_match(
#   miss = all_pop_share_mbz,
#   ref = unique_mbz,
#   miss_name = "name",
#   ref_name = "mbz_name",
#   id_col = "musicBrainzID"
# )
# 
# all <- left_join_coalesce(
#   all,
#   added_mbz,
#   by = "deezer_id",
#   col = c("musicBrainzID")
# )
# 
# 
# 
# 
# 
# 
# # some contact_names cannot be safely linked to a deezer name because there are duplicates
# # among those duplicates, check nr of reviews etc to see if some of them are way more popular
# # same logic as identify_deezer_duplicates.R!
# 
# tar_load(contacts)
# 
# # for each name in contacts, compute popularity of occurrences
# ## col_share: share of collection_counts
# ## prod_share: share of songs (?)
# ## gen_like_share: share of likes
# contacts <- as_tibble(contacts) %>% 
#   select(-c(contact_name_url, subtype_id, spotify_id,
#             mbz_id, freebase_id)) %>% 
#   # remove cases with no 
#   filter(product_count > 0 & gen_like_count > 0 & collection_count > 0) %>% 
#   group_by(contact_name) %>% 
#   mutate(col_share = collection_count / sum(collection_count),
#          prod_share = product_count / sum(product_count),
#          gen_like_share = gen_like_count / sum(gen_like_count))
# 
# 
# # ------- join to missing contact_data in all
# 
# # keep this condition for now: adding the other variables adds like 30 cases
# # but unsure about the cases (e.g., there are weird ones with very few albums)
# co_unique <- contacts %>% 
#   filter(col_share > 0.9) %>% 
#   select(contact_name, contact_id)
# 
# # important: subset all to unique names missing in contacts
# all_unique_co <- all %>% 
#   add_count(name) %>% 
#   filter(n == 1) %>% 
#   filter(is.na(contact_id))
# 
# added_contacts <- unique_name_match(
#   miss = all_unique_co,
#   ref = co_unique,
#   miss_name = "name",
#   ref_name = "contact_name",
#   id_col = "contact_id"
# )
# 
# all <- left_join_coalesce(
#   all,
#   added_contacts,
#   by = "deezer_id",
#   col = "contact_id"
# )
# 
# cleanpop(all)
# 
# 
# ## anti_join added contacts with missing in all to subset cases still missing
# t <- all_unique_co %>% 
#   anti_join(added_contacts, by = "deezer_id")
# 
# cleanpop(all)
# 
# sum(t[1:1000,]$pop)
# 





































