make_raw_isei <- function(survey_raw, isco_isei_file, isco_file, recode_file){
  
  # ------------------ load raw data
  isco_isei <- load_s3(isco_isei_file)
  isco <- load_s3(isco_file)
  claude_recode <- load_s3(recode_file)

  # 1. ----------------------- prepare survey
  
  # recode professions with sam's stuff
  survey <- survey_raw %>%
    left_join(claude_recode, by = "hashed_id") %>%
    filter(!is.na(profession_recode)) %>%
    select(hashed_id, profession_recode)

  ## 2. ----------------------- prepare isco and isei

  ## Prepare ISCO encoding table
  isco <- isco %>% 
    filter(libm != "2023") %>% 
    select(-id, -liste, -codeu) %>% 
    pivot_longer(libm:libf, values_to = "profession") %>% 
    mutate(
      isco = ssvaran # choose one isco condition, doesn't matter
    ) %>% 
    select(profession, isco) %>%
    distinct()

  isco_isei <- isco_isei %>%
    mutate(isco = as.character(isco)) %>% 
    # add a 0 when ISEI is 3 digits only (e.g. 110 to 0110)
    mutate(isco = ifelse(str_count(isco, "[0-9]") < 4, paste0(0,isco), isco)) %>% 
    right_join(isco, by = "isco")

  ## ------ join isei score to survey professions
  respondent_isei <- survey %>% 
    left_join(isco_isei, by = c(profession_recode = "profession")) %>% 
    filter(!is.na(isei)) %>% 
    select(hashed_id, isei)
  
  return(respondent_isei)
  
}





















