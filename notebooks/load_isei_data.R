

# Cleaning up data ------
clean_up <- function(string){
  string %>% 
    stringi::stri_trans_general("Latin-ASCII") %>% 
    tolower() %>% 
    str_replace_all(c("[-,\\.']" = " ")) %>% 
    str_replace_all(c("\\s+" = " "))
}


# survey_raw ------------------------------------------------------------------
## survey_prof derived from it
### --> survey results
tar_load(survey_raw)

# recode profession in survey
survey_raw <- survey_raw %>% 
  select(hashed_id, E_FR_prof_femme:E_FR_prof_retr_homme) %>% 
  pivot_longer(-hashed_id, values_to = "orig_survey_prof") %>% 
  filter(orig_survey_prof != "") %>% 
  mutate(orig_survey_prof = str_trim(orig_survey_prof)) %>% 
  select(-name) %>% 
  right_join(survey_raw, by = "hashed_id")

# list and counts of professions
all_profs <- survey_raw %>%  
  count(orig_survey_prof) %>% 
  mutate(survey_prof = clean_up(orig_survey_prof)) %>% 
  filter(!is.na(orig_survey_prof))


# pcs (L66) ------------------------------------------------------------------
### --> list of professions
### pcs_prof
### pcs_cod


# clean pcs
# récupérer noms de la pcs -- pour matcher avec réponses de survey
# clean
pcs <- load_s3("PCS2020/L66_Matrice_codification_PCS2020_collecte_2023.csv") 

pcs_1 <- pcs %>% 
  filter(libm != "2023") %>% 
  janitor::clean_names() %>% 
  select(libm:libf) %>% 
  pivot_longer(cols = c(libm:libf),
               names_to = "sexe", 
               values_to = "orig_pcs_prof") %>% 
  mutate(pcs_prof = clean_up(orig_pcs_prof)) %>% 
  distinct()

# pppcs, whatever this is
pppcs_2 <- pcs %>% 
  slice(-1) %>% 
  select(-libf, -liste, -natlib) %>% 
  pivot_longer(-libm) %>% 
  filter(!is.na(value)) %>% 
  group_by(value) %>% 
  slice(1) %>% 
  select(clean_pcs_prof = "libm", 
         PCS4 = "value")

## Prepare PCS encoding table
pcs_cod <- pcs %>% 
  select(-liste, -natlib, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "clean_pcs_prof") %>% 
  select(-name) %>% 
  pivot_longer(-clean_pcs_prof, 
               names_to = "condition_pcs", 
               values_to = "pcs4") %>% 
  distinct()



# openrefine ------------------------------------------------------------------

# we export and go through openrefine
# importer résultats openrefine pour compléter
or1 <- load_s3("records_w3/survey/pcs_openrefine1.csv") %>% 
  select(-survey_prof) %>% 
  filter(!(orig_survey_prof %in% matches$orig_survey_prof))


# isco (L72) ------------------------------------------------------------------
isco <- "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.xlsx" ## convert to csv and reexport to onyxia

## Prepare ISCO encoding table
isco_cod <- isco %>% 
  filter(libm != "2023") %>% 
  select(-id, -liste, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "clean_pcs_prof") %>% 
  select(-name) %>% 
  pivot_longer(-clean_pcs_prof, 
               names_to = "condition_isco", 
               values_to = "isco4") %>% 
  distinct()


# googlesheets recoprof ------------------------------------------------------------------
recoprof <- load_s3("PCS2020/handcoded_pcs4.csv") %>% 
  filter(!is.na(PCS4)) %>% 
  mutate(
    E_statut_pub_priv = ifelse(is.na(E_statut_pub_priv), "", E_statut_pub_priv),
    E_taille_entreprise = ifelse(is.na(E_taille_entreprise), "", E_taille_entreprise),
    E_position_pub = ifelse(is.na(E_position_pub), "", E_position_pub),
    E_position_priv = ifelse(is.na(E_position_priv), "", E_position_priv),
    E_encadre = ifelse(is.na(E_encadre), "", E_encadre)
  ) %>% 
  select(-n, -PCS3, -PCS2, -Inclassable)


# isco_isei ------------------------------------------------------------------
## --> just gets left-joined 




















