
make_respondent_socioecon <- function(respondent_streams, 
                                            # isei, 
                                            survey_raw){

  # isei
  respondent_isei <- respondent_streams %>%
    inner_join(isei) %>%
    group_by(dz_artist_id) %>%
    mutate(f = n_plays / sum(n_plays)) %>%
    summarise(n_isei = n(),
              respondent_mean_isei = sum(f * isei)) %>%
    filter(!is.na(respondent_mean_isei))

  # share of people with higher education
  educ <- survey_raw %>% 
    filter(E_diploma != "", !is.na(E_diploma)) %>% 
    mutate(higher_ed = as.numeric(E_diploma %in% c("Master, diplôme d'ingénieur.e, DEA, DESS", 
                                                   "Doctorat (y compris médecine, pharmacie, dentaire), HDR" ))) %>% 
    select(hashed_id, higher_ed) %>% 
    filter(!is.na(higher_ed))
  
  respondent_educ <- respondent_streams %>% 
    inner_join(educ, by = "hashed_id") %>% 
    group_by(dz_artist_feat_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    summarise(respondent_higher_ed_share = sum(f * higher_ed)) %>% 
    filter(!is.na(respondent_higher_ed_share))
  
  respondent_socioecon <- respondent_isei %>% 
    full_join(respondent_educ, by = "hashed_id")
  
  return(respondent_socioecon)
}


  
# compute mean age and gender of artists' listeners within respondents
make_respondent_demo <- function(respondent_streams, survey_raw){
  
  age <- survey_raw %>% 
    mutate(age = 2023 - E_birth_year) %>% 
    filter(!is.na(age), age < 100) %>% 
    select(hashed_id, age) 
  
  gender <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
    select(hashed_id, E_gender)
  
  # mean age of artist's audience
  audience_age <- respondent_streams %>% 
    inner_join(age, by = "hashed_id") %>% 
    group_by(dz_artist_feat_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    summarise(respondent_avg_age = sum(f * age))
  
  # share of females within artist's audience
  audience_share_female <- respondent_streams %>% 
    inner_join(gender, by = "hashed_id") %>% 
    group_by(dz_artist_feat_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    filter(E_gender == "Une femme") %>% 
    summarise(respondent_female_share = sum(f))
  
  
  respondent_demographics <- audience_age %>% 
    full_join(audience_share_female, by = "dz_artist_feat_id") %>% 
    mutate(dz_artist_id = as.character(dz_artist_feat_id)) %>% 
    select(dz_artist_id, 
           respondent_avg_age, 
           respondent_female_share)
    
  return(respondent_demographics)
  
}






