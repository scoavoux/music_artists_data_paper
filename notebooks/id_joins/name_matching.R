library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

# LOAD ALL AND CONTACTS -------------------------------------------

# for simplicity: unique deezer ids and names
all <- all %>% 
  distinct(deezer_id, .keep_all = T)

# subset missing contact ids
miss <- all %>% 
  filter(is.na(contact_id)) %>% 
  select(-contact_id)

# contacts
contacts <- load_s3("senscritique/contacts.csv")

contacts <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name) %>% 
  anti_join(all, by = "contact_id") # exclude found contact_ids


# ----------------- NAME MATCHING miss <=> contact name

### overview of occurrences
matches <- miss %>% 
  inner_join(contacts, by = c(name = "contact_name")) %>% 
  group_by(name) %>% 
  count() %>% 
  arrange(desc(n))

#### inner join missings with contacts by name
test <- miss %>% 
  inner_join(contacts, by = c(name = "contact_name")) %>% 
  group_by(name) %>% 
  mutate(n = n()) %>% 
  arrange(desc(n)) %>% 
  select(-contact_name)


#### keep unique perfect matches only
added_contacts <- test %>% 
  filter(n == 1) %>% 
  distinct(deezer_id, .keep_all = T)

pop(added_contacts)


non_unique <- matches %>% 
  filter(n > 1)

#### 

pop(t %>% filter(!is.na(contact_id)))

pop(added_contacts)

added_contacts <- added_contacts %>% 
  select(deezer_id, contact_id)

t <- all %>% 
  left_join(added_contacts, by = c("deezer_id", "name")) %>% 
  mutate(contact_id = coalesce(contact_id.x, contact_id.y))
  

# ---------------------- NAME MATCHING wiki

test <- miss %>% 
  inner_join(wiki, by = c(c(deezer_id = "deezerID"))) %>% 
  mutate(musicBrainzID = musicBrainzID.y)

sum(test$f_n_play)





# ------------------------------------------------------



sum(is.na(t$mbz_name) & !is.na(t$musicBrainzID))












