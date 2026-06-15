

# 
# survey_raw <- load_s3("records_w3/survey/RECORDS_Wave3_apr_june_23_responses_corrected.csv") %>% 
#              as_tibble() %>% 
#              filter(Progress == 100,
#                     country == "FR") %>% 
#   filter(E_gender %in% c("Un homme", "Une femme")) %>% 
#   mutate(gender = ifelse(E_gender == "Une femme", 1, 0)) %>% 
#   mutate(age = 2023 - E_birth_year) %>% 
#   left_join(raw_isei, by = "hashed_id") %>% 
#   filter(E_diploma != "", !is.na(E_diploma)) %>% 
#   mutate(higher_ed = ifelse(str_detect(E_diploma, "Licence|Master|Doctorat"), 1, 0),
#          graduate_ed = ifelse(str_detect(E_diploma, "Master|Doctorat"), 1, 0)) %>% 
#   select(hashed_id, age, gender, isei, higher_ed, graduate_ed)
#   
