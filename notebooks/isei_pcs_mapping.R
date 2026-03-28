

t <- isco_cod %>% 
  distinct(profession, isco4, .keep_all = T)


t %>% 
  add_count(profession) %>% 
  filter(n > 1)



profs <- assign_condition_isco(survey_professions)

profs <- profs %>% 
  select(hashed_id, survey_prof, condition_isco)


# # ------- 1. assign pcs conditions to handcoded and update pcs_cod with it
# handcoded_ready <- assign_condition_isco(handcoded_professions)
# 
# handcoded_ready <- handcoded_ready %>% 
#   select(profession = "survey_prof",
#          condition_isco,
#          pcs4) # replace with isco code
# 
# ### need to join pcs4 to isco (external table!)
# ### ppppppppcs comes into play
# 
# t <- handcoded_ready %>% 
#   inner_join(pcs_isco, by = c(pcs4 = "PCS4"))
# 


# REPLACE WITH ISCO CONDITIONS!
isco_cod <- bind_rows(isco_cod, handcoded_ready) %>% 
  distinct()


## ------- 2. coalesce openrefine recodes with survey professions
profs <- profs %>% 
  left_join(openrefine_raw, by = "survey_prof") %>% 
  mutate(survey_prof = coalesce(pcs_openrefine_prof, survey_prof)) %>% 
  select(-pcs_openrefine_prof) 


## ------ 3. append isei through joining by profession + isco condition
t <- profs %>% 
  left_join(isei_cod, by = c(c(survey_prof = "pcs_prof"), 
                            "condition_isco"))

prop_na(t)$isei # 30% of NAs (36% without hc, 38% with neither hc nor openrefine)



  




















