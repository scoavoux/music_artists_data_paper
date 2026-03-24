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
  select(-survey_prof) %>% 
  mutate(niveau = 4, 
         type="exact match")

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
  left_join(survey_professions, by = "orig_survey_prof") %>%
  inner_join(handcoded_professions, by = NULL) %>% # join by all common cols
  select(hashed_id, PCS4) %>% 
  left_join(pppcs_2, by = "PCS4") %>% 
  select(-PCS4)

handcoded

# (sum(matches$n_respondents) + nrow(handcoded))/objective

# Match databases ------------------------------------------------
# WHAT IS THIS??
survey_raw_2 <- exact_matches_or %>% 
  select(orig_survey_prof, 
         clean_pcs_prof = orig_pcs_prof, 
         pcs_niveau = niveau) %>% 
  right_join(survey_raw, by = "orig_survey_prof") %>% 
  left_join(handcoded, by = "clean_pcs_prof") #%>% 
mutate(clean_pcs_prof = coalesce(is.na(clean_pcs_prof), 
                                 clean_pcs_prof2, 
                                 clean_pcs_prof)) %>% 
  select(-clean_pcs_prof2)


result <- profs %>% 
  left_join(pcs_cod, by = c("clean_pcs_prof", "condition_pcs")) %>% 
  left_join(isco_cod, by = c("clean_pcs_prof", "condition_isco")) %>% 
  left_join(isco_isei, by = "isco4") %>% 
  mutate(pcs1 = str_extract(pcs4, "^\\w"),
         pcs2 = str_extract(pcs4, "^\\w{2}"),
         pcs3 = str_extract(pcs4, "^\\w{3}"),
         isco1 = str_extract(isco4, "^\\w"),
         isco2 = str_extract(isco4, "^\\w{2}"),
         isco3 = str_extract(isco4, "^\\w{3}")) %>% 
  select(hashed_id, isei)











