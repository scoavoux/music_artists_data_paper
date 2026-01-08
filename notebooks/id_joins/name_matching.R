### test script for name-based matches

library(dplyr)
library(arrow)
library(ggplot2)

options(scipen = 99)


#### --------------- ADD MBZ NAMES TO WIKI

mbid_name <- load_s3("musicbrainz/mbid_name_alias.csv")

mbz_name <- mbid_name %>% 
  filter(type == "name") %>% 
  as_tibble() %>% 
  select(musicBrainzID = "mbid",
         mbz_name = "name")

wiki <- wiki %>% 
  left_join(mbz_name, by = "musicBrainzID")


# --------- SUBSET EVERYTHING NOT FOUND

# subset missing mbz ids
miss <- all %>% 
  filter(is.na(musicBrainzID)) %>% 
  select(-musicBrainzID)

# mbz from wiki
wiki <- wiki %>% 
  as_tibble() %>% 
  select(musicBrainzID, mbz_name) %>% 
  filter(!is.na(mbz_name)) %>% 
  anti_join(all, by = "musicBrainzID") # exclude found mbz ids


### overview of occurrences (DESC only)
matches <- miss %>% 
  inner_join(wiki, by = c(name = "mbz_name")) %>% 
  group_by(name) %>% 
  count() %>% 
  arrange(desc(n))

#### inner join missings with wiki mbz by name
added_mbz <- miss %>% 
  inner_join(wiki, by = c(name = "mbz_name")) %>% 
  group_by(name) %>% 
  mutate(n = n()) %>% 
  arrange(desc(n)) %>% 
  select(-mbz_name) %>% 
  filter(n == 1) %>% # keep unique matches only
  distinct(deezer_id, .keep_all = T)

pop(added_mbz)


### MERGE INTO ALL
added_mbz <- added_mbz %>% 
  select(deezer_id, musicBrainzID)

## before
all %>% 
  filter(!is.na(musicBrainzID)) %>% 
  nrow()

all <- all %>% 
  left_join(added_mbz, by = c("deezer_id", "name")) %>% 
  mutate(musicBrainzID = coalesce(musicBrainzID.x, musicBrainzID.y)) %>% 
  select(-c(musicBrainzID.x, musicBrainzID.y)) %>% 
  as_tibble()

#####-------------------------------------
#####-------------------------------------

# FUZZ 

## STILL MISSING
## DISTINCT FOR NOW FOR THE SAKE OF SIMPLICITY


# subset missing contact_ids of names who don't have 
# a contact_id elsewhere, e.g. balthazar
names_with_contact <- all %>%
  filter(!is.na(contact_id)) %>%
  distinct(name)

missing_contact_after_consol <- all %>%
  anti_join(names_with_contact, by = "name")

pop(missing_contact_after_consol)



# already 4-5% within the first 1000-2000 cases
pop(missing_mbz_after_consol[1:2000,])
pop(missing_contact_after_consol[1:2000,])


write_s3(missing_mbz_after_consol[1:2000,], "interim/missing_mbz_after_consol.csv")
write_s3(missing_contact_after_consol[1:10000,], "interim/missing_contact_after_consol.csv")


missing_contact_after_consol %>% 
  filter(name == "Young Thug")



## why does balthazar have a contact_id in all but not
## in missing contacts??

all %>% 
  filter(name == "Balthazar")

missing_contact_after_consol %>%
  filter(name == "Balthazar")








