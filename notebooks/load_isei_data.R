
# survey_raw --> survey results

# pcs (L66) --> list of professions
### pcs_prof
### pcs_cod

# openrefine

# isco (L72)

# googlesheets "recoprof" --> handcodes RA, no access so leave out for now

# isco_isei --> just gets left-joined


load_pcs <- function(file="PCS2020/L66_Matrice_codification_PCS2020_collecte_2023.csv"){
    
    pcs <- load_s3(file) %>% 
      janitor::clean_names() %>% 
      mutate(pcs_prof = str_normalize(pcs_prof))
    
    
    # clean pcs
    pcs_prof <- pcs %>% 
      janitor::clean_names() %>% 
      filter(libm != "2023") %>% 
      select(libm:libf) %>% 
      pivot_longer(cols = c(libm:libf),
                   names_to = "sexe", # select cols??
                   values_to = "pcs_prof") %>% # skip the "orig_pcs_prof" step
      distinct(pcs_prof)
    
    # pppcs
    pppcs <- pcs %>% 
      slice(-1) %>% 
      select(-libf, -liste, -natlib) %>% 
      pivot_longer(-libm) %>% 
      filter(!is.na(value)) %>% 
      group_by(value) %>% 
      slice(1) %>% 
      select(clean_pcs_prof = "libm", PCS4 = "value")
    
    return(pcs)
}


## Prepare PCS encoding table
pcs_cod <- pcs %>% 
  filter(libm != "2023") %>% 
  select(-liste, -natlib, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "clean_pcs_prof") %>% 
  select(-name) %>% 
  pivot_longer(-clean_pcs_prof, 
               names_to = "condition_pcs", 
               values_to = "pcs4") %>% 
  distinct()



## Prepare ISCO encoding table

isco_cod <- isco %>% 
  filter(libm != "2023") %>% 
  select(-id, -liste, -codeu) %>% 
  pivot_longer(libm:libf, values_to = "clean_pcs_prof") %>% 
  select(-name) %>% 
  pivot_longer(-clean_pcs_prof, 
               names_to = "condition_isco", 
               values_to = "isco4") %>% 
  distinct()




or1 <- load_s3("records_w3/survey/pcs_openrefine1.csv")
isco_raw <- "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.xlsx" ## convert to csv and reexport to onyxia













