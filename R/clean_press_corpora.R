clean_telerama <- function(telerama_file){
  
  require(tidyverse)
  
  telerama <- load_s3(paste0("french_media/", telerama_file))
  
  telerama <- separate(telerama,
                       PublicationName, 
                       into = c("source_name", "issue"), 
                       sep = ", ")
  
  telerama <- telerama %>% 
    
    # mutate existing and new vars
    mutate(
      
      across(everything(), ~str_trim(.x)),
      
      issue = str_remove(issue, "no. "),
      
      date = str_replace(DocHeader, 
                         "^.*?((vendredi|samedi) \\d+ \\w+ \\d{4}).*$", 
                         "\\1"),
      
      date = ymd(date),
      
      article_page = str_extract(DocHeader, "p. .*$"),
      
      DocHeader = str_remove(DocHeader,
                             ",?\\s*(vendredi|samedi) \\d+ \\w+ \\d{4} \\w+ mots, p. .*$"),
      
      Notes = str_remove(Notes, "Note\\(s\\) : ;"),
      
      reviewed_score = str_extract(Notes, "\\b\\dF\\b"),
      
      reviewed_score = ifelse(is.na(reviewed_score), 
                              str_extract(Text, "\\bHelas\\b$"), 
                              reviewed_score),
      
      reviewed_score = ifelse(is.na(reviewed_score), 
                              str_extract(Text, "\\b\\dT\\b"), 
                              reviewed_score),
      
      article_type = ifelse(!is.na(reviewed_score), "review", NA),
      
      reviewed_artist = str_extract(Notes, "^.*? /") %>% str_remove(" /"),
      
      reviewed_title =  str_extract(Notes, "/ .*? >") %>% str_remove_all("[/>]") %>% str_trim(),
      
      source_origin = "europresse",
      
      source = "telerama",
      
      url = NA
      
    ) %>% 
    
    rename(article_title = "TitreArticle",
           article_text = "Text",
           article_annex = "Notes",
           article_author = "Authors",
           section = "DocHeader") %>% 
    
    as_tibble()
  
  print("telerama completed")
  
  return(telerama)
  
}


clean_lemonde <- function(lemonde_filepath, simulation = SIMULATION){
  
  require(stringr)
  require(dplyr)
  
  if (!simulation) {
  
  lemonde <- vector("list", length = 12L)
  
  # there are some small errors (bad dates in parsed data); we correct them below
  for(i in 1:12){
    lemonde[[i]] <- load_s3(paste0("french_media/", lemonde_filepath, 10L:22L,".csv")[i])
    print(i)
  }

  lemonde <- lapply(lemonde, function(x) {
    x <- mutate(x,
                is_article = as.logical(is_article),
                annee = as.numeric(annee),
                date = ymd(publidate)) %>%
      fill(publidate)
    return(x)
  }
  )

  lemonde <- bind_rows(lemonde)
  
  }
  
  else {
    
    lemonde <- load_s3("french_media/lemonde_raw.csv")
    
    lemonde <- lemonde %>% 
      mutate(is_article = as.logical(is_article),
             annee = as.numeric(annee),
             date = ymd(publidate)) %>%
      fill(publidate)
    
  }
  
  lemonde <- lemonde %>% 
    
    filter(str_detect(rubrique, "[Mm]usique")) %>% 
    filter(is_article) %>%     # attention, il y a plein de rubriques variées
    
    mutate(source = "lemonde",
           source_origin = "scraping") %>% 
    
    rename(section = "rubrique",
           article_author = "auteur",
           article_title = "titre",
           article_excerpt = "chapo",
           article_text = "texte") %>%
    
    select(-c(annee, publiheure, is_article, file)) %>% 
    
    as_tibble()
  
  print("le monde completed")
  
  return(lemonde)

}

## Le Figaro

clean_lefigaro <- function(lefigaro_file, simulation = SIMULATION) {
  
  if (!simulation) {
    
  s3 <- initialize_s3()
  
  tmp <- tempfile(fileext = ".csv")
  
  s3$download_file(
    Bucket = "scoavoux",
    Key = paste0("french_media/", lefigaro_file),
    Filename = tmp
  )
  
  lefigaro <- readr::read_csv(tmp)
  
  }
  
  else {
    
    lefigaro <- load_s3(paste0("french_media/",lefigaro_file))
    
  }
  
  lefigaro <- lefigaro %>% 
    
    filter(year > 2009) %>% 

    filter(rubrique == "Culture") %>% 

    mutate(date = ymd(date),
           source = "lefigaro") %>% 

    select(date,
           source,
           section = "rubrique",
           article_title = "titre",
           article_author = "auteur",
           article_text = "texte")
  
  print("le figaro completed")
  
  return(lefigaro)
  
}



clean_liberation <- function(liberation_file, simulation = SIMULATION){
  
  if (!simulation) {
    
  s3 <- initialize_s3()
  
  tmp <- tempfile(fileext = ".csv")
  
  s3$download_file(
    Bucket = "scoavoux",
    Key = paste0("french_media/", liberation_file),
    Filename = tmp
  )
  
  liberation <- readr::read_csv(tmp)
  
  }
  
  else {
    
    liberation <- load_s3(paste0("french_media/",liberation_file))
    
    
  }
  
  liberation <- liberation %>% 

    filter(rubrique %in% c("Culture", "Culture | Musique", "Musique")) %>% 

    rename(date = "publidate",
           section = "rubrique",
           article_author = "auteur",
           article_title = "titre",
           article_excerpt = "chapo",
           article_text = "texte") %>% 
    
    mutate(source = "liberation",
           date = ymd(date)) %>% 
    
    filter(date > "2009-12-31") %>% 
    
    select(-publiheure)
  
  print("liberation completed")
  
  return(liberation)
  
}


bind_press_corpora <- function(telerama_file, lefigaro_file, 
                               liberation_file, lemonde_filepath,
                               bert_reviews_file){
  
  bert_reviews <- load_s3(bert_reviews_file)
  
  telerama <- clean_telerama(telerama_file)
  lefigaro <- clean_lefigaro(lefigaro_file)
  liberation <- clean_liberation(liberation_file)
  lemonde <- clean_lemonde(lemonde_filepath)
  
  press_corpus <- bind_rows(telerama, lefigaro, liberation, lemonde) %>%
    filter(!is.na(article_text)) %>%
    mutate(article_text = paste(article_title, ".\n", article_text)) %>%
    select(source, article_text) %>% 
    distinct(article_text, .keep_all = T) %>% # REMOVE DUPLICATE TEXTS
    mutate(article_id = row_number()) %>%
    filter(!str_detect(article_text, "^NA")) %>%  # delete articles when title starts with NA
    as_tibble()
  
  # select prescriptive music reviews from BERT prediction
  press_corpus <- press_corpus %>% 
    inner_join(bert_reviews, by = "article_id") %>% 
    filter(prediction == "prescriptive") %>% 
    select(-prediction)
  
  # export to csv for NER processing
  write_s3(press_corpus, "interim/press/press_corpus.csv")
  
  return(press_corpus)
  
}


# # load entities file separately
# extracted_ents <- load_s3("press/extracted_ents_1203.csv")
# 
# # names to drop
# press_outliers_checked <- load_s3("press/press_outliers_checked_1003.csv")
# 
# # aliases to update
# ents_without_match_checked <- load_s3("press/ents_without_match_checked_1003.csv")
# 
# 
# extracted_ents %>% as_tibble()
# press_outliers_checked
# ents_without_match_checked
# 










