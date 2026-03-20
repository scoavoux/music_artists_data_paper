


make_respondent_demo <- function(respondent_streams, survey_raw){
  
  ages <- survey_raw %>% 
    mutate(age = 2023 - E_birth_year) %>% 
    filter(!is.na(age), age < 100) %>% 
    select(hashed_id, age) 
  
  genders <- survey_raw %>% 
    filter(E_gender %in% c("Un homme", "Une femme")) %>% 
    select(hashed_id, E_gender)
  
  # mean age of artist's audience
  audience_ages <- respondent_streams %>% 
    group_by(dz_artist_feat_id) %>% 
    mutate(f = n_plays / sum(n_plays)) %>% 
    summarise(average_audience_age = sum(f * age))
  
  # share of females within artist's audience
  audience_share_female <- respondent_streams %>% 
    inner_join(genders) %>% 
    group_by(dz_artist_feat_id) %>% 
    mutate(f = n_plays / sum(l_plays)) %>% 
    filter(E_gender == "Une femme") %>% 
    summarise(share_women_in_audience = sum(f))
  
  respondent_age_gender <- full_join(audience_ages, audience_share_female) %>% 
    
  
  return(respondent_age_gender)
  
}
