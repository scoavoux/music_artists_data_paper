# maybe create a big mapping table from survey to pcs?
# not caring about n respondents yet

survey_professions
pcs_professions

survey_professions <- survey_professions %>% 
  left_join(pcs_professions, by = c(survey_prof = "pcs_prof"))

prop_na(survey_professions)[8]

pcs_cod
openrefine_raw

# 2. add openrefine matches --------------------------------------------------
# for cases not found in exact maches
t <- survey_professions %>%
  anti_join(openrefine_raw, by = "survey_prof") %>% 
  mutate(match_type = "openrefine")

openrefine_raw
# mapping_table <- bind_rows(
#   exact_matches,
#   openrefine_matches
# ) %>%
#   distinct(survey_prof, .keep_all = TRUE)



# 3. add hand-coded cases
handcoded_professions


### pcs label to pcs code mapping
tail(pcs_cod)

























