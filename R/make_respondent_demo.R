# map survey professions to isco scores for isei
make_raw_isei <- function(survey_raw, isco_isei_file, isco_file, recode_file){
  
  # ------------------ load raw data
  isco_isei <- load_s3(isco_isei_file)
  isco <- load_s3(isco_file)
  claude_recode <- load_s3(recode_file)
  
  # 1. ----------------------- prepare survey
  
  # recode professions with sam's stuff
  survey <- survey_raw %>%
    mutate(hashed_id = as.character(hashed_id)) %>% 
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

# compute average isei from raw isei
make_respondent_isei <- function(respondent_streams, raw_isei){

  # make avg isei for artists
  respondent_isei <- respondent_streams %>%
    inner_join(raw_isei, by = "hashed_id") %>%
    group_by(dz_artist_id) %>%
    mutate(f = n_plays / sum(n_plays, na.rm = T)) %>%
    summarise(respondent_n_valid_isei = n(),
              respondent_mean_isei = sum(f * isei, na.rm = T))

  return(respondent_isei)
}


# make 2 higher education variables from survey responses
make_respondent_educ <- function(survey_raw, respondent_streams){
  
  # make share of respondents with higher education for artists
  educ <- survey_raw %>% 
    filter(E_diploma != "", !is.na(E_diploma)) %>% 
    mutate(higher_ed = ifelse(str_detect(E_diploma, "Licence|Master|Doctorat"), 1, 0),
           graduate_ed = ifelse(str_detect(E_diploma, "Master|Doctorat"), 1, 0),
           hashed_id = as.character(hashed_id)) %>% 
    select(hashed_id, higher_ed, graduate_ed)
  
  respondent_educ <- respondent_streams %>% 
    inner_join(educ, by = "hashed_id") %>% 
    group_by(dz_artist_id) %>% 
    mutate(f = n_plays / sum(n_plays, na.rm = T)) %>% 
    summarise(respondent_higher_ed_share = sum(f * higher_ed, na.rm = T),
              respondent_graduate_ed_share = sum(f * graduate_ed, na.rm = T))
  
  return(respondent_educ)
  
}

# compute mean age and gender of artists' listeners within respondents
# and bind all demographics together
make_respondent_demo <- function(respondent_streams, survey_raw, 
                                 respondent_educ, respondent_isei){
  
  age <- survey_raw %>% 
    mutate(age = 2023 - E_birth_year,
           hashed_id = as.character(hashed_id)) %>% 
    filter(!is.na(age), age < 100) %>% 
    select(hashed_id, age) 
  
  gender <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
    mutate(hashed_id = as.character(hashed_id)) %>% 
    select(hashed_id, E_gender)
  
  # mean age of artist's respondents
  respondent_age <- respondent_streams %>% 
    inner_join(age, by = "hashed_id") %>% 
    group_by(dz_artist_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    summarise(respondent_mean_age = sum(f * age, na.rm = T))
  
  # share of females within artist's respondents
  respondent_share_female <- respondent_streams %>% 
    inner_join(gender, by = "hashed_id") %>% 
    group_by(dz_artist_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    filter(E_gender == "Une femme") %>% 
    summarise(respondent_female_share = sum(f, na.rm = T))
  
  respondent_demographics <- respondent_age %>% 
    
    full_join(respondent_share_female, by = "dz_artist_id") %>% 
    full_join(respondent_educ, by = "dz_artist_id") %>% 
    full_join(respondent_isei, by = "dz_artist_id") %>% 
    
    select(dz_artist_id, 
           respondent_mean_age, 
           respondent_female_share,
           respondent_higher_ed_share,
           respondent_graduate_ed_share,
           respondent_mean_isei,
           respondent_n_valid_isei)

  return(respondent_demographics)
  
}


















