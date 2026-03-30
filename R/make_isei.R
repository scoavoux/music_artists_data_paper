make_raw_isei <- function(survey_raw, isco_isei_file, isco_file, openrefine_file){
  

  # ------------------ load raw data
  isco_isei <- load_s3(isco_isei_file)
  isco <- load_s3(isco_file)
  openrefine <- load_s3(openrefine_file)
  
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
    mutate(survey_prof = normalize_job(survey_prof)) %>% 
    filter(survey_prof != "") %>% 
    select(hashed_id, survey_prof, starts_with("E_"))
  
  # convert E_ variables to isco conditions
  survey <- survey %>% 
    assign_condition_isco() %>% 
    select(hashed_id, survey_prof, condition_isco)
  
  
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
  
  
  # 3. ------------------------- prepare openrefine recodes
  openrefine <- openrefine %>% 
    as_tibble() %>% 
    mutate(survey_prof = normalize_job(orig_survey_prof),
           pcs_openrefine_prof = normalize_job(orig_pcs_prof)) %>% 
    filter(!pcs_openrefine_prof %in% survey$survey_prof) %>% 
    select(-c(orig_survey_prof, orig_pcs_prof, n)) %>% 
    distinct()
  
  ## ------- coalesce openrefine recodes with survey professions
  survey <- survey %>% 
    left_join(openrefine, by = "survey_prof") %>% 
    mutate(survey_prof = coalesce(pcs_openrefine_prof, survey_prof)) %>% 
    select(-pcs_openrefine_prof) 
  
  
  ## ------ append isei through joining by profession + isco condition
  respondent_isei <- survey %>% 
    left_join(isei_cod, by = c(c(survey_prof = "profession"), 
                               "condition_isco")) %>% 
    select(hashed_id, isei)
  
  return(respondent_isei)
  
}

























