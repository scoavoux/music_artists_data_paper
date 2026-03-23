library(janitor)
library(stringr)

# recode profession in survey
survey <- survey_raw %>% 
  select(hashed_id, E_FR_prof_femme:E_FR_prof_retr_homme) %>% 
  pivot_longer(-hashed_id, values_to = "orig_survey_prof") %>% 
  filter(orig_survey_prof != "") %>% 
  mutate(orig_survey_prof = str_normalize(orig_survey_prof)) %>% 
  select(-name) %>% 
  right_join(survey_raw, by = "hashed_id")


all_profs <- survey %>%  
  count(orig_survey_prof) %>% 
  mutate(survey_prof = str_normalize(orig_survey_prof)) %>% 
  filter(!is.na(orig_survey_prof))

# Find matches between professions in survey and pcs table
matches <- all_profs %>% 
  inner_join(pcs, by = c(survey_prof = "pcs_prof")) %>% 
  select(-survey_prof) %>% 
  mutate(niveau = 4, 
         type="exact match")


# we export and go through openrefine
or1 <- or1 %>% 
  as_tibble() %>% 
  select(-survey_prof) #%>% 
  filter(!(orig_survey_prof %in% matches$orig_survey_prof))
  
matches <- or1 %>% 
  mutate(niveau = 4,
         type = "openrefine_handcoded") %>% 
  bind_rows(matches)


# Handcoded by research assistants
recoprof <- recoprof %>% 
  mutate(
    E_statut_pub_priv = ifelse(is.na(E_statut_pub_priv), "", E_statut_pub_priv),
    E_taille_entreprise = ifelse(is.na(E_taille_entreprise), "", E_taille_entreprise),
    E_position_pub = ifelse(is.na(E_position_pub), "", E_position_pub),
    E_position_priv = ifelse(is.na(E_position_priv), "", E_position_priv),
    E_encadre = ifelse(is.na(E_encadre), "", E_encadre)
  ) %>% 
  select(-n, -PCS3, -PCS2, -Inclassable)

pppcs <- pcs %>% 
  slice(-1) %>% 
  select(-libf, -liste, -natlib) %>% 
  pivot_longer(-libm) %>% 
  filter(!is.na(value)) %>% 
  group_by(value) %>% 
  slice(1) %>% 
  select(clean_pcs_prof = "libm", PCS4 = "value")

handcoded <- survey_raw %>% 
  select(hashed_id, orig_survey_prof, E_statut_pub_priv, E_taille_entreprise,
         E_position_pub, E_position_priv, E_encadre) %>% 
  left_join(select(all_profs, -n)) %>% 
  inner_join(recoprof) %>% 
  select(hashed_id, PCS4) %>% 
  left_join(pppcs) %>% 
  select(-PCS4)

(sum(matches$n) + nrow(handcoded))/objective

# Match databases ------
survey_raw <-  matches %>% 
  select(orig_survey_prof, 
         clean_pcs_prof = orig_pcs_prof, 
         pcs_niveau = niveau) %>% 
  right_join(survey_raw) %>% 
  left_join(rename(handcoded, clean_pcs_prof2 = "clean_pcs_prof")) %>% 
  mutate(clean_pcs_prof = ifelse(is.na(clean_pcs_prof), clean_pcs_prof2, clean_pcs_prof)) %>% 
  select(-clean_pcs_prof2)


profs <- select(survey_raw, 
                hashed_id, 
                clean_pcs_prof, 
                E_statut_pub_priv:E_position_priv, 
                E_encadre) %>% 
## --------- PLACEHOLDER FOR SUPER UGLY RECODE BLOCK ---------- ##


  
  
isco_isei_raw <- load_s3("PCS2020/isco_isei.csv") 

## OUTPUT: user to isei score table
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



## chatgpt model
survey_raw %>%
  extract_professions() %>%
  normalize_labels() %>%
  match_to_pcs() %>%
  apply_context_rules() %>%
  map_to_isco() %>%
  attach_isei()









