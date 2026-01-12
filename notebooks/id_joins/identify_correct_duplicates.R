# among the duplicate deezer names for which there is one single contact_id,
# find the cases where one duplicate is much more popular than the others
# try different thresholds

options(scipen = 99)



# subset unique contact names
## because those are the ones we can clearly assign to a deezer artist
## ONLY FOR IDENTIFYING NAMES TO CHECK!!
unique_contacts <- contacts %>% 
  select(-spotify_id) %>% 
  distinct(contact_name, .keep_all = T)


# subset duplicate deezer names



### list all duplicate names
dup <- all %>% 
  count(name) %>% 
  filter(n > 1) %>% 
  arrange(desc(n))

### filter different deezer artists (distinct ids!) with duplicate names
all_dup <- all %>% 
  distinct(deezer_id, name, .keep_all = T) %>% ## CHANGE THIS! (?)
  inner_join(dup, by = "name")

all_dup %>% 
  filter(name == "Lomepal")

# for each duplicate name, compute fraction of streams held by one homonym
test <- all_dup %>% 
  group_by(name) %>% 
  mutate(pop_share = pop / sum(pop))

test %>% 
  filter(name == "Angèle")

# join with unique contacts
# but filter out the clear cases: how?

# aha! filter the clear cases with missing contact_ids
t <- test %>% 
  filter(pop_share > 0.95) %>% 
  filter(is.na(contact_id))

t <- t %>%
  inner_join(unique_contacts, by = c(name = "contact_name"))

sum(t$pop)























