# ----- distinct target: enriching all with patches!

# patch_to_all <- function(patch, all) {
#   
#   all <- all %>%
#     left_join(patch, by = c("deezer_id", "name")) %>%
#     mutate_if(is.integer, as.character) %>%   # contact_id to str
#     mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>%
#     select(-c(contact_id.x, contact_id.y)) %>%
#     as_tibble()
#   
#   return(all)
# }
  
