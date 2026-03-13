clean_telerama <- function(file){
  
  require(tidyverse)
  
  telerama <- load_s3(paste0("french_media/",file))
  
  telerama <- separate(telerama,
                       PublicationName, 
                       into = c("source_name", "issue"), 
                       sep = ", ")
  
  
  telerama <- telerama %>% 
    
    # clean_dates() %>% 
    
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
  
  head(telerama)
  
  return(telerama)
  
}


clean_lemonde <- function(filepath){
  
  require(logging)
  require(stringr)
  require(dplyr)
  
  # Le Monde
  lemonde <- vector("list", length = 12L)
  
  # there are some small errors (bad dates in parsed data); we correct them below
  
  for(i in 1:12){
    lemonde[[i]] <- load_s3(paste0("french_media/", filepath, 10L:22L,".csv")[i])
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
  

}

## Le Figaro

clean_lefigaro <- function(file) {
  
  s3 <- initialize_s3()
  s3$download_file("scoavoux",
                   paste0("french_media/",file), 
                   paste0("data/temp/",file)
                   )
  
  lefigaro <- read_csv(paste0("data/temp/",file))
  
  file.remove(paste0("data/temp/",file))
  
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
  
  return(lefigaro)
  
}



clean_liberation <- function(file){
  
  require(logging)
  
  s3 <- initialize_s3()
  s3$download_file("scoavoux",
                   paste0("french_media/",file), 
                   paste0("data/temp/",file)
                   )
  

  liberation <- read_csv(paste0("data/temp/",file))
  
  file.remove(paste0("data/temp/",file))
  
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
    
    select(-publiheure, -file)
  
  return(liberation)
  
}


bind_press_corpora <- function(...){
  
  press_corpus <- bind_rows(...) %>%
    filter(!is.na(article_text)) %>%
    mutate(article_text = paste(article_title, ".\n", article_text)) %>%
    select(source, article_text) %>% 
    distinct(article_text, .keep_all = T) %>% # REMOVE DUPLICATE TEXTS
    mutate(article_id = row_number()) %>%
    as_tibble()
  
  head(press_corpus)
  
  return(press_corpus)
  
}




