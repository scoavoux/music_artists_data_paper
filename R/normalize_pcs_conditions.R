## normalize survey variables
normalize_for_isco <- function(df) {
  
  df %>%
    mutate(across(everything(), ~na_if(.x, ""))) %>%
    
    mutate(
      E_position_priv = stringr::str_squish(E_position_priv),
      E_position_pub  = stringr::str_squish(E_position_pub),
      E_encadre       = stringr::str_squish(E_encadre)
    ) %>%
    
    mutate(
      # unify private + public into one dimension
      pos_group = case_when(
        E_position_priv == "Ingénieur(e), cadre d’entreprise" |
          E_position_pub == "Agent de catégorie A de la fonction publique" ~ "cadre",
        
        E_position_priv %in% c("Technicien(ne)", 
                               "Agent de maîtrise (y compris administrative ou commerciale)") |
          E_position_pub %in% c("Agent de catégorie B de la fonction publique", 
                                "Technicien(ne)") ~ "intermediate",
        
        E_position_priv %in% c("Employé(e) de bureau, de commerce, de services",
                               "Ouvrier (ouvrière) qualifié(e), technicien(ne) d'atelier",
                               "Manœuvre, ouvrier (ouvrière) spécialisé(e)") |
          E_position_pub %in% c("Agent de catégorie C de la fonction publique",
                                "Ouvrier (ouvrière) qualifié(e)",
                                "Manœuvre, ouvrier (ouvrière) spécialisé(e)") ~ "worker",
        
        TRUE ~ "none"
      ),
      
      encadre = case_when(
        stringr::str_detect(E_encadre, "^Oui") ~ "yes",
        E_encadre == "Non" ~ "no",
        TRUE ~ NA_character_
      )
    )
}

## assign an isco condition based on the normalized survey variables
assign_condition_isco <- function(df) {
  
  ## conversion table from normalized survey variables to isco conditions
  isco_rules <- tibble::tribble(
    ~priority, ~pos_group,     ~encadre, ~condition_isco,
    
    1, "cadre",        "yes", "ingcad_supv",
    2, "cadre",        "no",  "ingcad_nsupv",
    
    3, "intermediate", "yes", "tecam_supv",
    4, "intermediate", "no",  "tecam_nsupv",
    
    5, "worker",       "yes", "ouvemp_supv",
    6, "worker",       "no",  "ouvemp_nsupv",
    
    7, "none",         "yes", "nr_supv",
    8, "none",         "no",  "nr_nsupv",
    
    99, NA, NA, "ssvaran"
  )
  
  df_norm <- normalize_for_isco(df)
  
  df_norm %>%
    left_join(isco_rules, by = c("pos_group", "encadre")) %>%
    
    # fallback if no match
    mutate(
      condition_isco = if_else(
        is.na(condition_isco),
        "ssvaran",
        condition_isco
      )
    )
}













