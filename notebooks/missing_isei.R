
## load survey dataset with job variables
## anti join with raw_isei


## try sth: impute mean value for each pcs category (ingcad, ouvemp...)


jobvars <- c("E_situation_prof",
             "E_statut_pub_priv",
             "E_taille_entreprise",
             "E_position_pub",
             "E_position_priv",
             "E_encadre")

tar_load(survey_raw)
tar_load(raw_isei)

survey_professions <- survey_raw %>% 
  pivot_longer(cols = c(E_FR_prof_femme:E_FR_prof_retr_homme), 
               values_to = "survey_prof") %>% 
  mutate(survey_prof = normalize_job(survey_prof)) %>% 
  filter(survey_prof != "") %>% 
  select(hashed_id, survey_prof, all_of(jobvars))


valid_isei <- raw_isei %>% 
  filter(!is.na(isei))

missing_prof <- survey_professions %>% 
  anti_join(valid_isei, by = "hashed_id") %>% 
  distinct(survey_prof, .keep_all = T)

missing_prof <- missing_prof %>% 
  assign_condition_isco() %>% 
  select(hashed_id, survey_prof, condition_isco)


### ISEI and pcs conditions
survey_professions <- survey_professions %>% 
  assign_condition_isco() %>% 
  select(hashed_id, survey_prof, condition_isco)

valid_isei <- raw_isei %>% 
  filter(!is.na(isei)) %>% 
  inner_join(survey_professions, by = "hashed_id")

mean_isei <- valid_isei %>% 
  rename(condition_pcs = "condition_isco") %>% 
  group_by(condition_pcs) %>% 
  summarise(mean_isei = median(isei, na.rm = T)) %>% 
  filter(condition_pcs != "ssvaran")



# ------------------------------------------
isco_isei_file = "PCS2020/isco_isei.csv"
isco_file = "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.csv"
openrefine_file = "records_w3/survey/pcs_openrefine1.csv"

# ------------------ load raw data
isco_isei <- load_s3(isco_isei_file)
isco <- load_s3(isco_file)
openrefine <- load_s3(openrefine_file)
  

isco <- isco %>% 
  distinct(profession, isco4, .keep_all = T)

write.csv2(missing_prof, "data/missing_professions.csv")









 