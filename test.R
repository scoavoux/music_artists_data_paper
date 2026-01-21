


  ## prepare reference table
  ref_clean <- contacts %>%
    as_tibble() %>%
    mutate(across(where(is.integer), as.character)) %>%
    select(contact_id, contact_name) %>%
    filter(!is.na(contact_name)) #%>%
    #anti_join(all, by = setNames(rlang::as_string(contact_id),
                                 # rlang::as_string(contact_id)))
  
  ref_clean

  ## rows in all missing IDs
  miss <- all %>%
    add_count(name) %>%
    filter(n == 1) %>% 
    filter(is.na(contact_id))
  
  miss

  ref_clean
  
  ## unique name-based matches
  matches <- miss %>%
    inner_join(ref_clean,
               by = c(name = "contact_name")) #%>%
    add_count(name, name = "n_all") %>%
    add_count(contact_name, name = "n_ref")
    #filter(n_all == 1, n_ref == 1)
  
  matches

  # subset wanted cols
  id_y <- paste0(rlang::as_string(ref_id), ".y")
  

  matches <- matches %>%
    select(
      !!all_name,
      !!ref_name, 
      !!rlang::as_string(ref_id) := !!rlang::sym(id_y),
      deezer_id
    ) 
  
  return(matches)
  
