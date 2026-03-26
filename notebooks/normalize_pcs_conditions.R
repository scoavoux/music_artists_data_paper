normalize_conditions <- function(df, pcs_rules) {
  
  df %>%
    
    # 1. Clean empty strings everywhere
    mutate(across(everything(), ~na_if(.x, ""))) %>%
    
    # 2. Normalize key columns (light normalization)
    mutate(
      E_statut_pub_priv   = stringr::str_squish(E_statut_pub_priv),
      E_taille_entreprise = stringr::str_squish(E_taille_entreprise),
      E_position_priv     = stringr::str_squish(E_position_priv),
      E_position_pub      = stringr::str_squish(E_position_pub),
      E_encadre           = stringr::str_squish(E_encadre)
    ) %>%
    
    # 3. Derive normalized variables
    mutate(
      
      # ---- statut ----
      statut = case_when(
        E_statut_pub_priv == "Vous travaill(i)ez, sans être rémunéré(e), avec un membre de votre famille" ~ "aid_fam",
        stringr::str_detect(E_statut_pub_priv, "À votre compte") ~ "indep",
        stringr::str_detect(E_statut_pub_priv, "fonction publique") ~ "public",
        stringr::str_detect(E_statut_pub_priv, "Salarié") ~ "private",
        TRUE ~ NA_character_
      ),
      
      # ---- taille entreprise ----
      taille = case_when(
        E_taille_entreprise %in% c(
          "Une seule personne : vous travaillez seul(e) / vous travailliez seul(e)",
          "Entre 2 et 10 personnes"
        ) ~ "0_9",
        E_taille_entreprise == "Entre 11 et 49 personnes" ~ "10_49",
        E_taille_entreprise == "50 personnes ou plus" ~ "50_plus",
        TRUE ~ NA_character_
      ),
      
      # ---- position privée ----
      pos_priv = case_when(
        E_position_priv == "Ingénieur(e), cadre d’entreprise" ~ "cadre",
        E_position_priv == "Technicien(ne)" ~ "tech",
        E_position_priv == "Agent de maîtrise (y compris administrative ou commerciale)" ~ "am",
        E_position_priv == "Employé(e) de bureau, de commerce, de services" ~ "emp",
        E_position_priv == "Ouvrier (ouvrière) qualifié(e), technicien(ne) d'atelier" ~ "ouv_q",
        E_position_priv == "Manœuvre, ouvrier (ouvrière) spécialisé(e)" ~ "ouv_nq",
        TRUE ~ NA_character_
      ),
      
      # ---- position publique ----
      pos_pub = case_when(
        E_position_pub == "Agent de catégorie A de la fonction publique" ~ "A",
        E_position_pub %in% c(
          "Agent de catégorie B de la fonction publique",
          "Technicien(ne)"
        ) ~ "B",
        E_position_pub %in% c(
          "Agent de catégorie C de la fonction publique",
          "Ouvrier (ouvrière) qualifié(e)",
          "Manœuvre, ouvrier (ouvrière) spécialisé(e)"
        ) ~ "C",
        TRUE ~ NA_character_
      ),
      
      # ---- encadrement ----
      encadre = case_when(
        stringr::str_detect(E_encadre, "^Oui") ~ "yes",
        E_encadre == "Non" ~ "no",
        TRUE ~ NA_character_
      )
    )

}

assign_condition_pcs <- function(df, rules = pcs_rules) {
  
  df_norm <- normalize_conditions(df)
  
  # Apply rules row-wise
  result <- purrr::map_dfr(seq_len(nrow(df_norm)), function(i) {
    
    row <- df_norm[i, ]
    
    match <- rules %>%
      dplyr::filter(
        (is.na(statut)   | statut   == row$statut),
        (is.na(taille)   | taille   == row$taille),
        (is.na(pos_priv) | pos_priv == row$pos_priv),
        (is.na(pos_pub)  | pos_pub  == row$pos_pub)
      ) %>%
      dplyr::arrange(priority) %>%
      dplyr::slice(1)
    
    dplyr::bind_cols(row, match["condition_pcs"])
  })
  
  return(result)
}

survey_norm <- assign_condition_pcs(survey_professions)

handcoded_ready <- assign_condition_pcs(handcoded_professions)
















