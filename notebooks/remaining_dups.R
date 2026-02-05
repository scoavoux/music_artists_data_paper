library(dplyr)
library(stringr)

tar_load(all_enriched)

# -----------------------------------------------


## DELETE COLLECTION_COUNT == 0 before counting dups

## compute pop_share and col_share before everything else

## run this after all enrichment steps so i have complete cases ? --> !


# --------------------------------------------------------------------------------

tar_load(all_enriched)

all <- all_enriched %>% 
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") #%>% 
  #filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)
  

# compute score share columns
# could add mbz too, but it concerns almost no cases
all <- all %>% 
  #filter(!is.na(contact_id)) %>%
  group_by(deezer_id) %>% 
  mutate(dz_col_share = collection_count / sum(collection_count)) %>% 
  ungroup() %>% 
  group_by(contact_id) %>% 
  mutate(co_pop_share = pop / sum(pop)) %>% 
  ungroup() 

to_keep_dz <- all %>% 
  filter(n_deezer > 1) %>% # duplicates
  group_by(deezer_id) %>%
  mutate(dz_col_share = collection_count / sum(collection_count)) %>% # create score_share 
  filter(dz_col_share == max(dz_col_share)) ## formerly score_share > threshold

to_keep_co <- all %>% 
  filter(n_co > 1) %>% # duplicates
  group_by(contact_id) %>%
  mutate(co_pop_share = collection_count / sum(collection_count)) %>%
  filter(co_pop_share == max(co_pop_share))

to_keep_mbz <- all %>% 
  filter(n_mbz > 1) %>% # duplicates
  group_by(musicbrainz_id) %>%
  mutate(mbz_pop_share = collection_count / sum(collection_count)) %>%
  filter(mbz_pop_share == max(mbz_pop_share))


all_enriched %>% 
  filter(deezer_id == 10336)


to_keep <- to_keep_co %>% 
  select(-c(n_deezer, n_co, n_mbz, dz_col_share, co_pop_share))

upd <- all_enriched %>% 
  rows_update(to_keep, by = "contact_id")
  


test <- to_keep_dz %>% 
  rows_update(to_keep_co, by = "deezer_id") %>% 
  rows_update(to_keep_mbz, by = "deezer_id") %>% 
  rows
  


# bind "winners"
all_enriched <- all_enriched %>% 
  anti_join(to_keep_dz, by = "deezer_id") %>% 
  bind_rows(to_keep_dz) %>% 
  anti_join(to_keep_co, by = "deezer_id") %>% 
  bind_rows(to_keep_co) %>% 
  anti_join(to_keep_mbz, by = "deezer_id") %>% 
  bind_rows(to_keep_mbz)

  

t <- all_enriched %>% 
  filter(!is.na(contact_id) & !is.na(musicbrainz_id)) %>% 
  add_count(deezer_id, name = "n_deezer") %>%
  add_count(contact_id, name = "n_co") %>%
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  filter(n_deezer > 1 | n_co > 1 | n_mbz > 1)


test <- t %>% 
  filter(!is.na(contact_id) & !is.na(musicbrainz_id))


# ------------------------------------------------------------------
tar_load(all_enriched)

all <- all_enriched %>% 
  group_by(deezer_id) %>% 
  mutate(
    dz_col_share = if_else(
      !is.na(deezer_id),
      collection_count / sum(collection_count, na.rm = TRUE),
      NA_real_
    )
  ) %>% 
  ungroup() %>% 
  group_by(contact_id) %>% 
  mutate(
    co_pop_share = if_else(
      !is.na(contact_id),
      pop / sum(pop, na.rm = TRUE),
      NA_real_
    )
  ) %>% 
  ungroup() %>% 
  group_by(musicbrainz_id) %>% 
  mutate(
    mbz_pop_share = if_else(
      !is.na(musicbrainz_id),
      pop / sum(pop, na.rm = TRUE),
      NA_real_
    )
  ) %>% 
  ungroup()


# ------------------------------------------------------

allcount <- all %>% 
  add_count(deezer_id, name = "n_dz") %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  mutate(
    keep_dz  = is.na(deezer_id) | n_dz == 1,
    keep_co  = is.na(contact_id) | n_co == 1,
    keep_mbz = is.na(musicbrainz_id) | n_mbz == 1
  ) %>% 
  mutate(.row_id = row_number())


# FILTER CONFLICTS
dz_conflicts <- allcount %>% 
  filter(!is.na(deezer_id), n_dz > 1) %>% 
  group_by(deezer_id) %>% 
  mutate(keep_dz = dz_col_share == max(dz_col_share, na.rm = TRUE)) %>% 
  ungroup() #%>% 
  #select(.row_id, keep_dz)

co_conflicts <- allcount %>% 
  filter(!is.na(contact_id), n_co > 1) %>% 
  group_by(contact_id) %>% 
  mutate(keep_co = co_pop_share == max(co_pop_share, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(.row_id, keep_co)

mbz_conflicts <- allcount %>% 
  filter(!is.na(musicbrainz_id), n_mbz > 1) %>% 
  group_by(musicbrainz_id) %>% 
  mutate(keep_mbz = mbz_pop_share == max(mbz_pop_share, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(.row_id, keep_mbz)

allcount$keep_dz[dz_conflicts$row_id] <- dz_conflicts$keep_dz
allcount$keep_co[co_conflicts$row_id] <- co_conflicts$keep_co
allcount$keep_mbz[mbz_conflicts$row_id] <- mbz_conflicts$keep_mbz


# -------------------------------------------------------

all_dedup <- allcount %>% 
  filter(keep_dz & keep_co & keep_mbz)

table(allcount$co_pop_share)

cleanpop(all_dedup)













