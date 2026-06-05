
library(tidyr)


# ------------------ load raw data
isco_isei <- load_s3("interim/prod/isco_isei.csv")
isco <- load_s3("interim/prod/L72_Matrice_codification_ISCO_collecte_2023.csv")

claude_recode <- read.csv("data/professions_recodees.csv")

claude_recode <- claude_recode %>% 
  as_tibble() %>% 
  mutate(profession_declaree = normalize_job(profession_declaree),
         profession_recode = normalize_job(profession_recode))

tar_load(survey_raw)

# 1. ----------------------- prepare survey
survey <- survey_raw %>% 
  select(hashed_id,
         E_FR_prof_femme:E_FR_prof_retr_homme,
         E_statut_pub_priv,
         E_taille_entreprise,
         E_position_pub,
         E_position_priv,
         E_encadre) %>% 
  pivot_longer(cols = c(E_FR_prof_femme:E_FR_prof_retr_homme), 
               values_to = "survey_prof") %>% 
  mutate(survey_prof = normalize_job(survey_prof),
         hashed_id = as.character(hashed_id)) %>% 
  filter(survey_prof != "") %>% 
  assign_condition_isco() %>% 
  select(hashed_id, survey_prof, condition_isco)

# recode professions with sam's stuff
survey_rec <- survey %>% 
  left_join(claude_recode, by = "hashed_id") %>% 
  select(-c(profession_declaree, survey_prof)) %>% 
  filter(!is.na(profession_recode))


## 2. ----------------------- prepare isco and isei

## Prepare ISCO encoding table
isco <- isco %>% 
  filter(libm != "2023") %>% 
  select(-id, -liste, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "profession") %>% 
  mutate(profession = normalize_job(profession)) %>% 
  select(-name) %>% 
  pivot_longer(-profession,
               names_to = "condition_isco", 
               values_to = "isco4") %>% 
  distinct()

# isco to isei 
isco_isei <- isco_isei %>% 
  mutate(isco4 = as.character(isco)) %>% 
  select(-isco) %>% 
  as_tibble()

## put ISEI into isco_cod directly
isei_cod <- isco %>% 
  inner_join(isco_isei, by = "isco4") %>% 
  select(-isco4)


## ------ append isei through joining by profession + isco condition
respondent_isei <- survey_rec %>% 
  left_join(isei_cod, by = c(c(profession_recode = "profession"), 
                             "condition_isco")) %>% 
  filter(!is.na(isei)) %>% 
  select(hashed_id, isei)








