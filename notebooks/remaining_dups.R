library(dplyr)
library(stringr)

tar_load(all_enriched)
cleanpop(all_enriched)
# -----------------------------------------------

## DELETE COLLECTION_COUNT == 0 before counting dups

## compute pop_share and col_share before everything else

## run this after all enrichment steps so i have complete cases ? --> !


# ------------------------------------------------------------------

# compute shares to determine winners

all <- all_enriched %>% 
  
  group_by(deezer_id) %>% 
  mutate(dz_col_share = collection_count / sum(collection_count, na.rm = TRUE))  %>% 
  ungroup() %>% 
  
  group_by(contact_id) %>%
  mutate(co_pop_share = pop / sum(pop, na.rm = TRUE)) %>%
  ungroup() %>%

  group_by(musicbrainz_id) %>%
  mutate(mbz_pop_share = pop / sum(pop, na.rm = TRUE)) %>%
  ungroup()

# ------------------------------------------------------

# only keep conflicts to speed up conflict identification
dz_conflicts <- all %>% 
  add_count(deezer_id, name = "n_dz") %>% 
  mutate(keep_dz = is.na(deezer_id) | n_dz == 1) %>% 
  filter(keep_dz == FALSE)

co_conflicts <- all %>% 
  add_count(contact_id, name = "n_co") %>% 
  mutate(keep_co = is.na(contact_id) | n_co == 1) %>% 
  filter(keep_co == FALSE)

mbz_conflicts <- all %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  mutate(keep_mbz = is.na(musicbrainz_id) | n_mbz == 1) %>% 
  filter(keep_mbz == FALSE)

# ---------------------------------------------------------

dz_losers <- dz_conflicts %>% 
  group_by(deezer_id) %>% 
  mutate(keep_dz = dz_col_share == max(dz_col_share, na.rm = TRUE)) %>% 
  filter(keep_dz == FALSE)

co_losers <- co_conflicts %>% 
  group_by(contact_id) %>% 
  mutate(keep_co = co_pop_share == max(co_pop_share, na.rm = TRUE)) %>% 
  filter(keep_co == FALSE)

mbz_losers <- mbz_conflicts %>% 
  group_by(musicbrainz_id) %>% 
  mutate(keep_mbz = mbz_pop_share == max(mbz_pop_share, na.rm = TRUE)) %>% 
  filter(keep_mbz == FALSE)

# -------------------------------------------------------

# remove from all

all_dedup <- all %>% 
  anti_join(dz_losers, by = c("deezer_id", "contact_id", "musicbrainz_id")) %>% 
  anti_join(co_losers, by = c("deezer_id", "contact_id", "musicbrainz_id")) %>% 
  anti_join(mbz_losers, by = c("deezer_id", "contact_id", "musicbrainz_id")) %>% 
  
  # count remaining duplicates
  add_count(deezer_id, name = "n_deezer") %>% 
  add_count(contact_id, name = "n_co") %>% 
  add_count(musicbrainz_id, name = "n_mbz") %>% 
  
  # set NA
  mutate(
    n_deezer = ifelse(n_deezer > 1000, NA, n_deezer),
    n_co = ifelse(n_co > 1000, NA, n_co),
    n_mbz = ifelse(n_mbz > 1000, NA, n_mbz)
  )


all_dedup <- all_dedup %>% 
  filter(n_deezer == 1 | is.na(n_deezer)) %>% 
  filter(n_co == 1 | is.na(n_co)) %>% 
  filter(n_mbz == 1 | is.na(n_mbz))
  
  
## check filtered out cases
t <- all %>% 
  anti_join(all_dedup, by = c("deezer_id", "contact_id", "musicbrainz_id"))














