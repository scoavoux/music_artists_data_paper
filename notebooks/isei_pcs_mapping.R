library(fuzzyjoin)


survey_norm <- normalize_conditions(survey_professions)
handcoded_norm <- normalize_conditions(handcoded_professions)




## add openrefine
profs <- profs %>% 
  left_join(openrefine_raw, by = "survey_prof") %>% 
  mutate(survey_prof = coalesce(pcs_openrefine_prof, survey_prof)) # openrefine if available, else survey_prof



## add pcs4
t <- profs %>% 
  left_join(pcs_cod, by = c(c(survey_prof = "pcs_prof"), 
                            "condition_pcs"))

prop_na(t)$pcs4 # 36% NAs (38% without openrefine)



# add handcoded
# googlesheets recoprof ------------------------------------------------------------------
handcoded_professions

missing_pcs4 <- t %>% 
  filter(is.na(pcs4))

test <- missing_pcs4 %>% 
  inner_join(handcoded_professions, by = c("survey_prof", "E_statut_pub_priv",
                                          "E_taille_entreprise", "E_position_pub",
                                          "E_position_priv", "E_encadre"))


handcoded_professions %>% 
  add_count(survey_prof, E) %>% 
  filter(n > 1)


prop_na(test)$pcs4 










