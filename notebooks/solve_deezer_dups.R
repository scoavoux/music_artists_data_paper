dedup <- function(all, contacts, ref_id){
  
  ref_id <- rlang::sym(ref_id)

  coll_count <- coll_count <- contacts %>% 
    select(contact_id, collection_count)

  ref_dup <- all %>% 
    #filter(!is.na(contact_id)) %>% 
    left_join(coll_count, by = "contact_id") %>% 
    add_count(!!ref_id) %>% 
    filter(n > 1) %>% # filter duplicates
    #filter(collection_count > 0) %>% 
        group_by(contact_id) %>%
    mutate(max_pop = max(pop, na.rm = TRUE), # create pop_share
           pop_share = pop / sum(pop)) %>%
    arrange(desc(max_pop), desc(pop))
  
  to_keep <- ref_dup %>% 
    filter(pop_share > 0.9) %>% 
    select(-c(pop_share, collection_count, max_pop, n))

  
  return(to_keep)
}

x <- dedup(all = all_enriched,
           contacts = contacts,
           ref_id = "contact_id")
x

y <- dedup(all = all_enriched,
           contacts = contacts,
           ref_id = "musicbrainz_id")
y


all_1 <- all_enriched %>%
  anti_join(x, by = "contact_id") %>%
  bind_rows(x)

all_2 <- all_1 %>%
  anti_join(y, by = "musicbrainz_id") %>%
  bind_rows(y)





library(sjmisc)
library(dplyr)

tar_load(all_enriched)
tar_load(contacts)


## work separately for co and mbz: subset !is.na(co)
## then !is.na(mbz)
## then solve deezer through collection count!

coll_count <- contacts %>% 
  select(contact_id, collection_count)

co_dup <- all_enriched %>% 
  #filter(!is.na(contact_id)) %>% # just deduplicate ALL artists?? maybe even before patching!
  left_join(coll_count, by = "contact_id") %>% 
  #add_count(deezer_id, name = "n_deezer") %>% 
  #add_count(musicbrainz_id, name = "n_mbz") %>% 
  add_count(contact_id, name = "n_co") %>% 
  
  # filter duplicates
  filter(n_co > 1) %>%
  #filter(collection_count > 0) %>% # filter irrelevant artists
  # create pop_share
  group_by(contact_id) %>%
  mutate(max_pop = max(pop, na.rm = TRUE),
         pop_share = pop / sum(pop)) %>%
  arrange(desc(max_pop), desc(pop))


###### use pop_share to disambiguate!
co_to_keep <- co_dup %>% 
  filter(pop_share > 0.9)

## KEEP ONLY SELECTED WINNERS
final_df <- all_enriched %>%
  anti_join(co_to_keep, by = "contact_id") %>%
  bind_rows(co_to_keep)

## missing pop before 
a <- all_enriched %>%
  filter(!is.na(contact_id)) %>% 
  add_count(contact_id) %>% 
  filter(n > 1) %>% 
  distinct(deezer_id, .keep_all = T)

sum(a$pop)

## missing pop after
b <- final_df %>%
  filter(!is.na(contact_id)) %>% 
  add_count(contact_id) %>% 
  filter(n > 1) %>% 
  distinct(deezer_id, .keep_all = T)

sum(b$pop)







