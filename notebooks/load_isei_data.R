library(dplyr)
library(tidyr)
library(tidyverse)
library(stringi)


# recode profession in survey
survey_professions <- survey_raw %>% 
  pivot_longer(cols = c(E_FR_prof_femme:E_FR_prof_retr_homme), 
               values_to = "survey_prof") %>% 
  mutate(survey_prof = normalize_job(survey_prof)) %>% 
  filter(survey_prof != "") %>% 
  select(hashed_id, survey_prof, starts_with("E_"))


# pcs (L66) ------------------------------------------------------------------
### --> list of professions
### pcs_prof
### pcs_cod

# clean pcs
# récupérer noms de la pcs -- pour matcher avec réponses de survey
# clean
pcs_raw <- load_s3("PCS2020/L66_Matrice_codification_PCS2020_collecte_2023.csv") 

pcs_professions <- pcs_raw %>% 
  filter(libm != "2023") %>% 
  janitor::clean_names() %>% 
  select(libm:libf) %>% 
  pivot_longer(cols = c(libm:libf),
               names_to = "sex", 
               values_to = "pcs_prof") %>% 
  mutate(pcs_prof = normalize_job(pcs_prof),
         match_type = "exact") %>% 
  select(-sex) %>% 
  distinct()


# pppcs: à la fin, on matche
# dans le google sheets on a des professions non listées dans PCS
# on leur attribue un code PCS4
# pppcs prend un nom de profession pour chaque code pcs qui apparait dans la base
pppcs_2 <- pcs_raw %>% 
  slice(-1) %>% 
  select(-libf, -liste, -natlib) %>% 
  pivot_longer(-libm) %>% 
  filter(!is.na(value)) %>% 
  group_by(value) %>% 
  slice(1) %>% 
  select(pcs_prof = "libm", 
         PCS4 = "value")



## Prepare PCS encoding table
pcs_cod <- pcs_raw %>% 
  select(-liste, -natlib, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "pcs_prof") %>% 
  select(-name) %>% 
  mutate(pcs_prof = normalize_job(pcs_prof)) %>% 
  pivot_longer(-pcs_prof, 
               names_to = "condition_pcs", 
               values_to = "pcs4") %>% 
  distinct()


View(isco_rules)



# openrefine ------------------------------------------------------------------

# export and go through openrefine --- fuzzy matching
# map some survey professions to pcs professions
openrefine_raw <- load_s3("records_w3/survey/pcs_openrefine1.csv") 

openrefine_raw <- openrefine_raw %>% 
  as_tibble() %>% 
  mutate(survey_prof = normalize_job(orig_survey_prof),
         pcs_openrefine_prof = normalize_job(orig_pcs_prof)) %>% 
  filter(!pcs_openrefine_prof %in% survey_professions$survey_prof) %>% 
  select(-c(orig_survey_prof, orig_pcs_prof, n)) %>% 
  distinct()




# googlesheets recoprof ------------------------------------------------------------------
handcoded_professions <- load_s3("PCS2020/handcoded_pcs4.csv") %>% 
  filter(!is.na(PCS4), PCS4 != "X", PCS4 != "") %>% 
  mutate(
    E_statut_pub_priv = ifelse(is.na(E_statut_pub_priv), "", E_statut_pub_priv),
    E_taille_entreprise = ifelse(is.na(E_taille_entreprise), "", E_taille_entreprise),
    E_position_pub = ifelse(is.na(E_position_pub), "", E_position_pub),
    E_position_priv = ifelse(is.na(E_position_priv), "", E_position_priv),
    E_encadre = ifelse(is.na(E_encadre), "", E_encadre)
  ) %>% 
  select(-n, -PCS3, -PCS2, -Inclassable) %>% 
  rename(pcs4 = "PCS4") %>% 
  as_tibble()



# --- PCS to ISCO
pcs_isco <- read.csv("data/Matrice_PCS_ISCO_pour_programmes.csv",
         sep = ";")

pcs_isco <- pcs_isco %>% 
  as_tibble() %>% 
  select(PCS4, ISCO4)














