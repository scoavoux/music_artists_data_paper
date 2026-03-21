
load_pcs <- function(file="PCS2020/L66_Matrice_codification_PCS2020_collecte_2023.csv"){
    
    pcs <- load_s3(file)
    
    pcs_prof <- pcs %>% 
      janitor::clean_names() %>% 
      filter(libm != "2023") %>% 
      select(libm:libf) %>% 
      pivot_longer(cols = c(libm:libf),
                   names_to = "sexe", # select cols??
                   values_to = "pcs_prof") %>% 
      distinct(pcs_prof)
    
    
    # Cleaning up data ------
    pcs <- pcs %>% 
      distinct(pcs_prof) %>% 
      mutate(pcs_prof = str_normalize(pcs_prof))
    
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
isco_isei_raw <- load_s3("PCS2020/isco_isei.csv") 
isco_raw <- "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.xlsx" ## convert to csv and reexport to onyxia


isco_isei <- isco_isei %>% 
  rename(isco4 = "isco")


# handcodes RA
library(googlesheets4)
gs4_auth(email = "samuel.coavoux@gmail.com")

recoprof <- read_sheet("1DcVZkiNRS9XbLFJxl_qRtbF1Um9xKkothWOLuUb28vw", col_types = "c") %>% 
  filter(!is.na(PCS4))











