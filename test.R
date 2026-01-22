

all_reprex <- miss[1:10,] %>% 
  select(name, contact_name, deezer_id, contact_id)

dup <- tibble(name = c("Bramsito", "Bramsito"),
              contact_name = c(NA, NA),
              deezer_id = c(88, 99),
              contact_id = c(NA, NA))

all_reprex <- rbind(all_reprex, dup)


ref_reprex <- tibble(contact_name = c("Bramsito", "13 Block", "S.Pri Noir"),
                     contact_id = c(1,2,3) #,
                     #deezer_id = c(6,7,8)
                     ) %>% 
  mutate(contact_id = as.character(contact_id))




## prepare reference table
ref_clean <- ref_reprex %>%
  as_tibble() %>%
  select(contact_id, contact_name) %>%
  filter(!is.na(!!ref_name)) %>%
  anti_join(all_reprex, by = "contact_id") # %>% 
# distinct() # distinct perfect duplicates!!
ref_clean

## rows in all missing IDs
miss <- all_reprex %>%
  filter(is.na(contact_id))


matches <- miss %>%
  inner_join(ref_clean,
             by = c(name = "contact_name")) %>%
  add_count(name, name = "n_all") %>% 
  filter(n_all == 1) # CRUCIAL: keep only unique names

matches

miss
ref_clean

## unique name-based matches
matches <- miss %>%
  inner_join(ref_clean,
             by = c(name = "contact_name")) %>%  #%>%
  add_count(name, name = "n_all") %>%
  add_count(contact_name, name = "n_ref")
  #filter(n_all == 1, n_ref == 1)

matches

# subset wanted cols
id_y <- paste0(rlang::as_string(ref_id), ".y")


matches <- matches %>%
  select(
    !!all_name,
    !!ref_name, 
    !!rlang::as_string(ref_id) := !!rlang::sym(id_y),
    deezer_id
  ) 

return(matches)


matches <- matches %>% 
  select(name, 
         contact_name, 
         deezer_id, 
         contact_id = "contact_id.y") %>% 
  recode(contact_id = as.character(matches$contact_id))
  
# matches$contact_id <- as.character(matches$contact_id)

all_reprex %>% 
  rows_update(matches, by = "deezer_id")


all_reprex
ref_reprex

t <- patch_names(all=miss, 
            ref=ref_clean, 
            ref_id="contact_id", 
            ref_name="contact_name", 
            all_name="name")

all_reprex
ref_reprex




all_update <- all %>% 
  rows_update(t, by = "deezer_id")


contacts_names_patch %>% 
  add_count(contact_id, deezer_id) %>% 
  filter(n > 1)


######################
######################

matches <- miss %>%
  inner_join(ref_clean,
             by = c(name = "contact_name")) %>%
  group_by(name) %>%
  mutate(n = n()) %>%
  filter(n == 1) %>%
  ungroup()


matches <- miss %>%
  inner_join(ref_clean,
             by = c(name = "contact_name")) %>%
  add_count(name) %>% 
  filter(n == 1)




# -----------------------------------------

tar_load(mbz_deezer)
tar_load(contacts)
tar_load(manual_search)
tar_load(wiki)
tar_load(artists)



t <- artists %>% 
  left_join(mbz_deezer, by = "deezer_id") %>% 
  left_join(contacts, by = c(musicbrainz_id = "mbz_id"))

contacts %>% 
  filter(contact_name == "Orelsan") %>% 
  as_tibble()

mbz_deezer %>% 
  filter(mbz_name == "Orelsan") %>% 
  as_tibble()

t <- artists %>% 
  left_join(mbz_deezer, by = "deezer_id") %>% 
  left_join(contacts, by = c(musicbrainz_id = "mbz_id")) %>% 
  #left_join(manual_search, by = c(deezer_id = "artist_id")) %>% 
  left_join(wiki, by = "deezer_id") %>% # added wiki for names
  #mutate(contact_id = coalesce(contact_id.x, contact_id.y)) %>% 
  select(name, contact_name, deezer_id, 
         musicbrainz_id, contact_id, pop) %>% 
  as_tibble()








