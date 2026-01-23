### test notebook for name-based matching
library(targets)
library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)

tar_source("R")

all <- load_s3("interim/consolidated_artists.csv")
all <- all %>% 
  as_tibble()

# contacts keys
contacts <- load_s3("senscritique/contacts.csv")
contacts <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name, mbz_id, spotify_id)


# case 1: there is a lomepal in contacts but being a
# duplicate in all, his contact_id wasn't appended
all %>% 
  filter(name == "Lomepal")

contacts %>% 
  filter(contact_name == "Lomepal")

# case 2: one balthazar duplicate in all has a valid contact_id,
# but since there are others, he ends up in the list of missing contact_ids
all %>% 
  filter(name == "Balthazar")

contacts %>% 
  filter(contact_name == "Balthazar")



## filter unique names with no contact_id (not only missing contact_ids!)
## distinct for now (?)

all_no_dups <- all %>%
  add_count(name) %>%
  filter(n == 1) %>%
  select(-n)


## maybe get rid of perfect matches here already?

# get rid of names who have a contact_id 
# elsewhere, e.g. balthazar
names_with_contact <- all_no_dups %>%
  filter(!is.na(contact_id)) %>%
  distinct(name)

missing_contact_after_consol <- all_no_dups %>%
  anti_join(names_with_contact, by = "name")

pop(missing_contact_after_consol)


# already 4-5% within the first 1000-2000 cases
pop(missing_mbz_after_consol[1:10000,])
pop(missing_contact_after_consol[1:100000,])


#write_s3(missing_mbz_after_consol[1:10000,], "interim/missing_mbz_after_consol.csv")
#write_s3(missing_contact_after_consol[1:100000,], "interim/missing_contact_after_consol.csv")


# --------------------------------------------------
library(stringdist)
library(fuzzyjoin)

missing_contact_after_consol <- missing_contact_after_consol[1:50,]

contacts$name <- contacts$contact_name

# Perform fuzzy matching
matched_data <- stringdist_join(
  missing_contact_after_consol, contacts,
  by = c("name"),  # Column to match on
  mode = "left",  # Type of join: left, inner, or full
  method = "lv",  # Levenshtein
  max_dist = 0.05  # Maximum allowable distance for a match
) %>%
  group_by(name.x) %>%                 # name from missing_contact_after_consol
  slice_min(order_by = dist, n = 1, with_ties = FALSE) %>%
  ungroup()





# -------------------------------------------------------

### IDENTIFICATION OF MISSING CONTACT IDS


# subset missing contact ids
miss <- all %>% 
  filter(is.na(contact_id)) %>% 
  select(-contact_id)

# contacts
co <- contacts %>% 
  as_tibble() %>% 
  select(contact_id, contact_name) %>% 
  anti_join(all, by = "contact_id") # exclude found contact_ids


### overview of occurrences
### DESC ONLY
matches <- miss %>% 
  inner_join(co, by = c(name = "contact_name")) %>% 
  group_by(name) %>% 
  count() %>% 
  arrange(desc(n))

matches

# -------------------------------------------------------


install.packages("zoomerjoin")
library(zoomerjoin)


corpus_a <- missing_contact_after_consol[,"name"]
corpus_b <- na.omit(contacts[,"contact_name"])

names(corpus_b) <- "name"

euclidean_left_join(corpus_a, corpus_b)

