library(janitor)
library(stringr)
library(tidyr)

# Make hashed id / ISEI  pairing ------

# DATA ------------------------------------------------------------------------

# 4735 unique labels for 10412 individuals
# nrow(survey_professions)
# objective <- sum(survey_professions$n_respondents)
# objective

# 1. Exact matches between survey and pcs labels ---------------------------------
exact_matches <- survey_professions %>% 
  inner_join(pcs_1, by = c(survey_prof = "pcs_prof")) %>% 
  mutate(niveau = 4, 
         type="exact match")

exact_matches
# exact match finds 2509 for 7330 individuals
# nrow(matches)
# sum(matches$n_respondents)

# 2. add openrefine matches --------------------------------------------------
exact_matches_or <- openrefine_1 %>% 
  mutate(niveau = 4,
         type = "openrefine_handcoded") %>% 
  bind_rows(exact_matches)

# with first openrefine wave we are at 2787 labels for 7813 individuals
# nrow(exact_matches_or)
# sum(exact_matches_or$n_respondents)
# sum(matches$n_respondents)/objective


# 3. add handcoded cases --------------------------------------------------

# repartir du survey, récup ceux qui ont été recodés (googlesheets) par inner_join
handcoded <- survey_raw %>% 
  select(hashed_id, 
         orig_survey_prof, 
         E_statut_pub_priv, 
         E_taille_entreprise,
         E_position_pub, 
         E_position_priv, 
         E_encadre) %>% 
  left_join(survey_professions, by = "survey_prof") %>%
  inner_join(handcoded_professions, by = NULL) %>% # join by all common cols
  select(hashed_id, PCS4) %>% 
  left_join(pppcs_2, by = "PCS4") %>% 
  select(-PCS4)


# (sum(matches$n_respondents) + nrow(handcoded))/objective

# Match databases ------------------------------------------------
# WHAT IS THIS??
survey_matched <- exact_matches_or %>% 
  select(survey_prof, 
         pcs_prof, 
         niveau) %>% 
  right_join(survey_raw, by = "survey_prof") %>% 
  select(hashed_id, pcs_prof)


result <- survey_matched %>% 
  left_join(pcs_cod, by = c("pcs_prof")) %>% 
  left_join(isco_cod, by = c("pcs_prof")) %>% 
  left_join(isco_isei, by = "isco4") %>% 
  select(hashed_id, isei) %>% 
  distinct()







