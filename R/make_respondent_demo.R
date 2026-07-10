# map survey professions to isco scores for isei
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

# compute average isei from raw isei
# compute average isei from raw isei
make_respondent_isei <- function(respondent_streams, raw_isei){
  
  respondent_isei <- respondent_streams %>%
    inner_join(raw_isei, by = "hashed_id") %>%
    group_by(hashed_id) %>%
    mutate(user_artist_share = n_plays / sum(n_plays)) %>%
    ungroup() %>%
    group_by(dz_artist_id) %>%
    mutate(
      weight_abs = n_plays / sum(n_plays),
      weight_rel = user_artist_share / sum(user_artist_share)
    ) %>%
    summarise(
      audience_mean_isei = weighted.mean(isei, weight_abs, na.rm = TRUE),
      audience_mean_isei_rel = weighted.mean(isei, weight_rel, na.rm = TRUE),
      .groups = "drop"
    )
  
  respondent_isei
}


# make 2 higher education variables from survey responses
make_respondent_educ <- function(survey_raw, respondent_streams){
  
  educ <- survey_raw %>%
    filter(E_diploma != "", !is.na(E_diploma)) %>%
    mutate(
      higher_ed = ifelse(str_detect(E_diploma, "Licence|Master|Doctorat"), 1, 0),
      graduate_ed = ifelse(str_detect(E_diploma, "Master|Doctorat"), 1, 0)
    ) %>%
    select(hashed_id, higher_ed, graduate_ed)

  respondent_educ <- respondent_streams %>%
    inner_join(educ, by = "hashed_id") %>%
    group_by(hashed_id) %>%
    mutate(user_artist_share = n_plays / sum(n_plays)) %>%
    ungroup() %>%
    group_by(dz_artist_id) %>%
    mutate(
      weight_abs = n_plays / sum(n_plays),
      weight_rel = user_artist_share / sum(user_artist_share)
    ) %>%
    summarise(
      audience_higher_ed_share = weighted.mean(higher_ed, weight_abs, na.rm = TRUE),
      audience_higher_ed_share_rel = weighted.mean(higher_ed, weight_rel, na.rm = TRUE),
      audience_graduate_ed_share = weighted.mean(graduate_ed, weight_abs, na.rm = TRUE),
      audience_graduate_ed_share_rel = weighted.mean(graduate_ed, weight_rel, na.rm = TRUE),
      .groups = "drop"
    )
  
  respondent_educ
}

# compute mean age and gender of artists' listeners within respondents
make_respondent_demo <- function(respondent_streams, survey_raw,
                                 respondent_educ, respondent_isei,
                                 min_n_users){
  
  age <- survey_raw %>%
    mutate(age = 2023 - E_birth_year) %>%
    filter(!is.na(age), age < 100) %>%
    select(hashed_id, age)
  
  gender <- survey_raw %>%
    filter(E_gender %in% c("Un homme", "Une femme")) %>%
    mutate(
      gender = ifelse(E_gender == "Une femme", 1, 0)
    ) %>%
    select(hashed_id, gender)
  
  respondent_age <- respondent_streams %>%
    inner_join(age, by = "hashed_id") %>%
    group_by(hashed_id) %>%
    mutate(user_artist_share = n_plays / sum(n_plays)) %>%
    ungroup() %>%
    group_by(dz_artist_id) %>%
    mutate(
      weight_abs = n_plays / sum(n_plays),
      weight_rel = user_artist_share / sum(user_artist_share)
    ) %>%
    summarise(
      audience_mean_age = weighted.mean(age, weight_abs, na.rm = TRUE),
      audience_mean_age_rel = weighted.mean(age, weight_rel, na.rm = TRUE),
      .groups = "drop"
    )
  
  respondent_share_female <- respondent_streams %>%
    inner_join(gender, by = "hashed_id") %>%
    group_by(hashed_id) %>%
    mutate(user_artist_share = n_plays / sum(n_plays)) %>%
    ungroup() %>%
    group_by(dz_artist_id) %>%
    mutate(
      weight_abs = n_plays / sum(n_plays),
      weight_rel = user_artist_share / sum(user_artist_share)
    ) %>%
    summarise(
      audience_female_share = weighted.mean(gender, weight_abs, na.rm = TRUE),
      audience_female_share_rel = weighted.mean(gender, weight_rel, na.rm = TRUE),
      .groups = "drop"
    )
  
  respondent_demographics <- respondent_age %>%
    full_join(respondent_share_female, by = "dz_artist_id") %>%
    full_join(respondent_educ, by = "dz_artist_id") %>%
    full_join(respondent_isei, by = "dz_artist_id")
  
  respondent_demographics <- respondent_demographics %>%
    left_join(
      respondent_streams %>%
        select(dz_artist_id, n_users_respondent) %>%
        distinct(),
      by = "dz_artist_id"
    ) %>%
    mutate(
      across(
        starts_with("audience_"),
        ~ if_else(n_users_respondent < min_n_users, NA_real_, .)
      )
    ) %>%
    select(dz_artist_id, starts_with("audience_"))
  
  return(respondent_demographics)
}















