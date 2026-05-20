
### load and clean bert prediction 
### to append to press_corpus

df <- read.csv("data/predictions_music-reviews_bert-Mandrill-15h18-20-May_default_all-2.csv")

names(df)

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
  print(df$text[i])

}


  
  
  
  
  
  
  
  
  
  
  
  
  
  