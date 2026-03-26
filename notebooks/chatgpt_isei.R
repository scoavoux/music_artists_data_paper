library(dplyr)
library(stringr)
library(tidyr)

# ------------------------------------------------------------------------------
# 1. Normalization helper (apply ONCE per dataset)
# ------------------------------------------------------------------------------

normalize_job <- function(x) {
  x %>%
    str_to_lower() %>%
    str_squish()
}

# ------------------------------------------------------------------------------
# 2. Build unified mapping table (ALL matching strategies together)
# ------------------------------------------------------------------------------

build_mapping <- function() {
  
  exact <- pcs_1 %>%
    transmute(
      survey_prof = normalize_job(pcs_prof),
      clean_pcs_prof = normalize_job(pcs_prof),
      source = "exact"
    )
  
  openrefine <- openrefine_1 %>%
    transmute(
      survey_prof = normalize_job(survey_prof),
      clean_pcs_prof = normalize_job(pcs_prof),
      source = "openrefine"
    )
  
  bind_rows(exact, openrefine) %>%
    distinct(survey_prof, .keep_all = TRUE)  # priority = first row
}

# ------------------------------------------------------------------------------
# 3. Handcoded mapping (clean + minimal joins)
# ------------------------------------------------------------------------------

build_handcoded <- function(survey_raw) {
  
  survey_raw %>%
    mutate(survey_prof = normalize_job(survey_prof)) %>%
    inner_join(handcoded_professions, by = colnames(handcoded_professions)[-7]) %>%
    left_join(pppcs_2, by = "PCS4") %>%
    select(hashed_id, pcs_prof)
}


# ------------------------------------------------------------------------------
# 4. Replace case_when with lookup tables
# ------------------------------------------------------------------------------

# PCS condition lookup
pcs_conditions <- tribble(
  ~E_statut_pub_priv, ~E_taille_entreprise, ~condition_pcs,
  "indep_small", "0_9", "inde_0_9",
  "indep_small", "10_49", "inde_10_49",
  "indep_small", "50_plus", "inde_sup49"
)

# ISCO condition lookup (example structure)
isco_conditions <- tribble(
  ~position_group, ~encadre, ~condition_isco,
  "cadre", "yes", "ingcad_supv",
  "cadre", "no", "ingcad_nsupv",
  "intermediate", "yes", "tecam_supv",
  "intermediate", "no", "tecam_nsupv"
)

# helper recodes (reduce raw survey complexity early)
recode_inputs <- function(df) {
  df %>%
    mutate(
      statut_group = case_when(
        str_detect(E_statut_pub_priv, "compte") ~ "indep",
        str_detect(E_statut_pub_priv, "publique") ~ "public",
        TRUE ~ "private"
      ),
      encadre_bin = if_else(str_detect(E_encadre, "Oui"), "yes", "no")
    )
}

# ------------------------------------------------------------------------------
# 5. Main function
# ------------------------------------------------------------------------------

make_isei_data <- function(survey_raw) {
  
  # Normalize ONCE
  survey_raw <- survey_raw %>%
    mutate(survey_prof = normalize_job(survey_prof))
  
  mapping <- build_mapping()
  handcoded <- build_handcoded(survey_raw)
  
  # --------------------------------------------------------------------------
  # Merge mapping (priority: handcoded > others)
  # --------------------------------------------------------------------------
  
  survey <- survey_raw %>%
    left_join(mapping, by = "survey_prof") %>%
    left_join(handcoded, by = "hashed_id", suffix = c("", "_hc")) %>%
    mutate(
      clean_pcs_prof = coalesce(pcs_prof_hc, pcs_prof)
    ) %>%
    select(-clean_pcs_prof_hc)
  
  
  survey_raw %>% 
    left_join(handcoded, by = "hashed_id", suffix = c("", "_hc")) %>%
    select(ends_with("_hc"))
    
  # --------------------------------------------------------------------------
  # Derive conditions via lookup tables
  # --------------------------------------------------------------------------
  
  survey <- survey %>%
    recode_inputs() %>%
    left_join(pcs_conditions, by = c("statut_group")) %>%
    left_join(isco_conditions, by = c("encadre_bin"))
  
  # --------------------------------------------------------------------------
  # Final joins (PCS → ISCO → ISEI)
  # --------------------------------------------------------------------------
  
  result <- survey %>%
    left_join(pcs_cod, by = c("clean_pcs_prof", "condition_pcs")) %>%
    left_join(isco_cod, by = c("clean_pcs_prof", "condition_isco")) %>%
    left_join(isco_isei, by = "isco4") %>%
    transmute(
      hashed_id,
      isei
    )
  
  return(result)
}