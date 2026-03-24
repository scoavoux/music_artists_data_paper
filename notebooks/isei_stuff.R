library(janitor)
library(stringr)
library(tidyr)

# Make hashed id / ISEI  pairing ------
make_isei_data <- function(survey_raw){
  
  # DATA ------------------------------------------------------------------------
  

  # 4735 unique labels for 10412 individuals
  # nrow(survey_professions)
  # objective <- sum(survey_professions$n_respondents)
  # objective
  
  # 1. Exact matches between survey and pcs labels ---------------------------------
  exact_matches <- survey_professions %>% 
    inner_join(pcs_1, by = c(survey_prof = "pcs_prof")) %>% 
    select(-survey_prof) %>% 
    mutate(niveau = 4, 
           type="exact match")
  
  # exact match finds 2509 for 7330 individuals
  # nrow(matches)
  # sum(matches$n_respondents)

  # 2. add openrefine matches --------------------------------------------------
  exact_matches_or <- openrefine_1 %>% 
    mutate(niveau = 4,
           type = "openrefine_handcoded") %>% 
    bind_rows(exact_matches)
  
  # with first openrefine wave we are at 2787 labels for 7813 individuals
  # nrow(exact_matches_or)
  # sum(exact_matches_or$n_respondents)
  # sum(matches$n_respondents)/objective
   
  
  
  # 3. add handcoded cases --------------------------------------------------
  
  # repartir du survey, récup ceux qui ont été recodés (googlesheets) par inner_join
  handcoded <- survey_raw %>% 
    select(hashed_id, 
           orig_survey_prof, 
           E_statut_pub_priv, 
           E_taille_entreprise,
           E_position_pub, 
           E_position_priv, 
           E_encadre) %>% 
    left_join(survey_professions, by = "orig_survey_prof") %>%
    inner_join(handcoded_professions, by = NULL) %>% # join by all common cols
    select(hashed_id, PCS4) %>% 
    left_join(pppcs_2, by = "PCS4") %>% 
    select(-PCS4)

  handcoded
  
  # (sum(matches$n_respondents) + nrow(handcoded))/objective
  
  # Match databases ------------------------------------------------
  survey_raw_2 <- exact_matches_or %>% 
    select(orig_survey_prof, 
           clean_pcs_prof = orig_pcs_prof, 
           pcs_niveau = niveau) %>% 
    right_join(survey_raw, by = "orig_survey_prof") %>% 
    left_join(handcoded, by = "clean_pcs_prof") #%>% 
    mutate(clean_pcs_prof = coalesce(is.na(clean_pcs_prof), 
                                   clean_pcs_prof2, 
                                   clean_pcs_prof)) %>% 
    select(-clean_pcs_prof2)

  survey_raw_2
  
  
  profs <- survey_raw %>% 
    select(hashed_id, 
           clean_pcs_prof, 
           E_statut_pub_priv:E_position_priv, 
           E_encadre) %>% 
    filter(!is.na(clean_pcs_prof)) %>% 
    mutate(across(everything(), ~ifelse(.x == "", NA, .x))) %>% 
    mutate(condition_pcs = case_when(E_statut_pub_priv == "Vous travaill(i)ez, sans être rémunéré(e), avec un membre de votre famille" ~ "aid_fam", 
                                     E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" &
                                       E_taille_entreprise %in% c("Une seule personne : vous travaillez seul(e) / vous travailliez seul(e)",
                                                                  "Entre 2 et 10 personnes") ~ "inde_0_9", 
                                     E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" &
                                       E_taille_entreprise == "Entre 11 et 49 personnes" ~ "inde_10_49", 
                                     E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" &
                                       E_taille_entreprise == "50 personnes ou plus" ~ "inde_sup49", 
                                     E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" ~ "inde_nr", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") &
                                       E_position_priv == "Ingénieur(e), cadre d’entreprise" ~ "priv_cad", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") &
                                       E_position_priv == "Technicien(ne)" ~ "priv_tec", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") &
                                       E_position_priv == "Agent de maîtrise (y compris administrative ou commerciale)" ~ "priv_am", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") &
                                       E_position_priv == "Employé(e) de bureau, de commerce, de services" ~ "priv_emp", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") & 
                                       E_position_priv == "Ouvrier (ouvrière) qualifié(e), technicien(ne) d'atelier" ~ "priv_oq", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") & 
                                       E_position_priv == "Manœuvre, ouvrier (ouvrière) spécialisé(e)" ~ "priv_opq", 
                                     E_statut_pub_priv %in% c("Salarié(e) d’une entreprise (y compris d’une association ou de la Sécurité sociale)", 
                                                              "Salarié(e) d’un particulier") ~ "priv_nr", 
                                     E_statut_pub_priv == "Salarié(e) de la fonction publique (État, territoriale ou hospitalière)" &
                                       E_position_pub == "Agent de catégorie A de la fonction publique" ~ "pub_catA", 
                                     E_statut_pub_priv == "Salarié(e) de la fonction publique (État, territoriale ou hospitalière)" &
                                       E_position_pub %in% c("Agent de catégorie B de la fonction publique", 
                                                             "Technicien(ne)") ~ "pub_catB", 
                                     E_statut_pub_priv == "Salarié(e) de la fonction publique (État, territoriale ou hospitalière)" &
                                       E_position_pub %in% c("Agent de catégorie C de la fonction publique",
                                                             "Ouvrier (ouvrière) qualifié(e)",
                                                             "Manœuvre, ouvrier (ouvrière) spécialisé(e)") ~ "pub_catC", 
                                     E_statut_pub_priv == "Salarié(e) de la fonction publique (État, territoriale ou hospitalière)" ~ "pub_nr", 
                                     TRUE ~ "ssvaran"),
           condition_isco = case_when(E_position_priv == "Ingénieur(e), cadre d’entreprise" | 
                                        E_position_pub == "Agent de catégorie A de la fonction publique" & 
                                        E_encadre %in% c("Oui, mais ce n’est pas (ce n’était pas) ma tâche principale", 
                                                         "Oui, et c’est (c’était) ma tâche principale") ~ "ingcad_supv", 
                                      E_position_priv == "Ingénieur(e), cadre d’entreprise" | 
                                        E_position_pub == "Agent de catégorie A de la fonction publique" & 
                                        E_encadre == "Non" ~ "ingcad_nsupv", 
                                      E_position_priv %in% c("Technicien(ne)", 
                                                             "Agent de maîtrise (y compris administrative ou commerciale)") | 
                                        E_position_pub %in% c("Agent de catégorie B de la fonction publique", "Technicien(ne)") & 
                                        E_encadre %in% c("Oui, mais ce n’est pas (ce n’était pas) ma tâche principale", 
                                                                                                                                                                                                                                                           "Oui, et c’est (c’était) ma tâche principale") ~ "tecam_supv", 
                                      E_position_priv %in% c("Technicien(ne)", 
                                                             "Agent de maîtrise (y compris administrative ou commerciale)") | 
                                        E_position_pub %in% c("Agent de catégorie B de la fonction publique", 
                                                              "Technicien(ne)") & 
                                        E_encadre == "Non" ~ "tecam_nsupv", 
                                      E_position_priv %in% c("Employé(e) de bureau, de commerce, de services", 
                                                             "Ouvrier (ouvrière) qualifié(e), technicien(ne) d'atelier", 
                                                             "Manœuvre, ouvrier (ouvrière) spécialisé(e)") | 
                                        E_position_pub %in% c("Agent de catégorie C de la fonction publique", 
                                                              "Ouvrier (ouvrière) qualifié(e)",
                                                              "Manœuvre, ouvrier (ouvrière) spécialisé(e)") & 
                                        E_encadre %in% c("Oui, mais ce n’est pas (ce n’était pas) ma tâche principale", 
                                                         "Oui, et c’est (c’était) ma tâche principale") ~ "ouvemp_supv",
                                      
                                      E_position_priv %in% c("Employé(e) de bureau, de commerce, de services", 
                                                             "Ouvrier (ouvrière) qualifié(e), technicien(ne) d'atelier", 
                                                             "Manœuvre, ouvrier (ouvrière) spécialisé(e)") | 
                                        E_position_pub %in% c("Agent de catégorie C de la fonction publique", 
                                                              "Ouvrier (ouvrière) qualifié(e)", 
                                                                                                                                                                                                                                                 "Manœuvre, ouvrier (ouvrière) spécialisé(e)") & E_encadre == "Non" ~ "ouvemp_nsupv", 
                                      E_encadre %in% c("Oui, mais ce n’est pas (ce n’était pas) ma tâche principale", 
                                                       "Oui, et c’est (c’était) ma tâche principale") ~ "nr_supv", 
                                      
                                      E_encadre == "Non" ~ "nr_nsupv", 
                                      
                                      E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" &
                                        E_taille_entreprise %in% c("Une seule personne : vous travaillez seul(e) / vous travailliez seul(e)",
                                                                   "Entre 2 et 10 personnes") ~ "inde_0_9", 
                                      
                                      E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" &
                                        E_taille_entreprise %in% c("Entre 11 et 49 personnes", 
                                                                   "50 personnes ou plus") ~ "inde_sup10", 
                                      
                                      E_statut_pub_priv == "À votre compte (y compris gérant de société ou chef d’entreprise salarié)" ~ "inde_nr", 
                                      TRUE ~ "ssvaran")
    ) %>% 
    select(hashed_id, clean_pcs_prof, starts_with("condition_"))
  
  
  result <- profs %>% 
    left_join(pcs_cod, by = c("clean_pcs_prof", "condition_pcs")) %>% 
    left_join(isco_cod, by = c("clean_pcs_prof", "condition_isco")) %>% 
    left_join(isco_isei, by = "isco4") %>% 
    mutate(pcs1 = str_extract(pcs4, "^\\w"),
           pcs2 = str_extract(pcs4, "^\\w{2}"),
           pcs3 = str_extract(pcs4, "^\\w{3}"),
           isco1 = str_extract(isco4, "^\\w"),
           isco2 = str_extract(isco4, "^\\w{2}"),
           isco3 = str_extract(isco4, "^\\w{3}")) %>% 
    select(hashed_id, isei)
  
  return(result)
}


## chatgpt model
survey_raw %>%
  extract_professions() %>%
  normalize_labels() %>%
  match_to_pcs() %>%
  apply_context_rules() %>%
  map_to_isco() %>%
  attach_isei()









