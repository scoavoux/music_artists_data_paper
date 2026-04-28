make_respondent_isei <- function(respondent_streams, raw_isei){

  # make avg isei of artists
  respondent_isei <- respondent_streams %>%
    inner_join(raw_isei, by = "hashed_id") %>%
    group_by(dz_artist_id) %>%
    mutate(f = n_plays / sum(n_plays, na.rm = T)) %>%
    summarise(respondent_n_valid_isei = n(),
              respondent_mean_isei = sum(f * isei, na.rm = T))

  return(respondent_isei)
}



make_respondent_educ <- function(survey_raw, respondent_streams){
  
  # make share of respondents with higher education for artists
  educ <- survey_raw %>% 
    filter(E_diploma != "", !is.na(E_diploma)) %>% 
    mutate(higher_ed = ifelse(str_detect(E_diploma, "Licence|Master|Doctorat"), 1, 0)) %>% 
    mutate(graduate_ed = ifelse(str_detect(E_diploma, "Master|Doctorat"), 1, 0)) %>% 
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
make_respondent_demo <- function(respondent_streams, survey_raw, 
                                 respondent_educ, respondent_isei){
  
  age <- survey_raw %>% 
    mutate(age = 2023 - E_birth_year) %>% 
    filter(!is.na(age), age < 100) %>% 
    select(hashed_id, age) 
  
  gender <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
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
    
    full_join(respondent_share_female, by = "dz_artist_id") %>% # CHANGE ONCE dz_feat_id IS DEALT WITH
    full_join(respondent_educ, by = "dz_artist_id") %>% # CHANGE ONCE dz_feat_id IS DEALT WITH
    full_join(respondent_isei, by = "dz_artist_id") %>% # CHANGE ONCE dz_feat_id IS DEALT WITH
    
    rename(dz_artist_id = "dz_artist_id") %>%  # CHANGE ONCE dz_feat_id IS DEALT WITH
  
    select(dz_artist_id, 
           respondent_mean_age, 
           respondent_female_share,
           respondent_higher_ed_share,
           respondent_graduate_ed_share,
           respondent_mean_isei,
           respondent_n_valid_isei)

  return(respondent_demographics)
  
}


















