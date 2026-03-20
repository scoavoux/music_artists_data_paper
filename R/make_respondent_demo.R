

make_respondent_demo <- function(respondent_streams, survey_raw){
  
  age <- survey_raw %>% 
    mutate(age = 2023 - E_birth_year) %>% 
    filter(!is.na(age), age < 100) %>% 
    select(hashed_id, age) 
  
  gender <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
    select(hashed_id, E_gender)
  
  # mean age of artist's audience
  audience_ages <- respondent_streams %>% 
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
  
  respondent_age_gender <- full_join(audience_ages, audience_share_female, by = "dz_artist_feat_id") %>% 
    mutate(dz_artist_id = as.character(dz_artist_feat_id)) %>% 
    select(dz_artist_id, respondent_avg_age, respondent_female_share)
    
  return(respondent_age_gender)
  
}