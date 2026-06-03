
### load and clean bert prediction 
### to append to press_corpus

bert_review <- read.csv("data/predictions_music-reviews_bert-Mandrill-15h18-20-May_default_all-2.csv")

bert_review <- bert_review %>% 
  select("article_id",
         "prediction") %>% 
  as_tibble()

head(bert_review)

write_s3(bert_review, "press_files/bert_review_classif.csv")

t <- load_s3("press_files/bert_review_classif.csv")

# -------------------------------------------------

df <- df %>% 
  select("article_id", 
         "text",
         "prediction",
         "prescriptive",
         "non.prescriptive", 
         "prediction") %>% 
  as_tibble()

# delete NA articles
df <- df %>% 
  filter(!str_detect(text, "^NA"))

df_review <- df %>% 
  filter(prediction == "prescriptive")

for(i in 1:100){
  
  print(i)
  print(df_review$text[i])

}


  
  
  
  
  
  
  
  
  
  
  
  
  
  