# enrich the consolidated artists file "all" 
# with contact_ids through unique name matches


## GENERALIZE AS patch_names ??? i could actually
patch_contact_names <- function(all, contacts){
  
}

## prepare contacts
# contacts_ref <- contacts %>% 
#   as_tibble %>% 
#   mutate_if(is.integer, as.character) %>%   # contact_id to str
#   select(contact_id, contact_name) %>% # id cols only
#   filter(!is.na(contact_name)) %>% # contacts with names only
#   anti_join(all, by = "contact_id") # contacts not already in all only

  


# ---------- BREAK UP HELPERS INTO ORIGINAL CODE AGAIN (CLARITY!!)
## more lines of code, but transparency is key


# added_contacts <- miss %>%
#   inner_join(contacts, by = c(name = "contact_name")) %>%
#   group_by(name) %>%
#   mutate(n = n()) %>%
#   arrange(desc(n)) %>%
#   select(-contact_name) %>%
#   filter(n == 1) %>% # keep unique matches only
#   distinct(deezer_id, .keep_all = T) # NEED THIS??????
# 


# ----- distinct target: enriching all with patches!

# all <- all %>% 
#   left_join(added_contacts, by = c("deezer_id", "name")) %>% 
#   mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
#   select(-c(contact_id.x, contact_id.y)) %>% 
#   as_tibble()





























