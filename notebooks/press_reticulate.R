library(reticulate)
library(dplyr)
library(readr)
library(stringr)
library(purrr)
library(tidyr)
library(tibble)

# point to your Python env if needed
use_virtualenv(".venv", required = TRUE)

spacy <- import("spacy")

# load model (must match Python exactly)
nlp <- spacy$load("fr_core_news_lg", disable = list("tok2vec", "parser", "lemmatizer"))


presse <- read_csv("/Users/pol/Downloads/press_corpus.csv")

df <- presse


# reticulate passes tuples as Python tuples
docs <- nlp$pipe(
  reticulate::tuple(df$article_text, df$article_id),
  as_tuples = TRUE,
  batch_size = as.integer(8),
  n_process = as.integer(4)
)


result <- list()

i <- 1

for (item in docs) {
  doc <- item[[1]]
  idx <- item[[2]]
  
  ents <- doc$ents
  
  if (length(ents) > 0) {
    for (ent in ents) {
      if (ent$label_ != "LOC") {
        result[[i]] <- list(
          article_id = idx,
          ent_name = ent$text,
          ner_type = ent$label_
        )
        i <- i + 1
      }
    }
  }
}



ner_df <- bind_rows(result)


remove_tiretdusix <- function(string) {
  string %>%
    str_replace("^-", "") %>%
    str_trim()
}

ner_df <- ner_df %>%
  filter(!is.na(ent_name)) %>%
  mutate(ent_name = map_chr(ent_name, remove_tiretdusix))


ner_df <- ner_df %>%
  group_by(ent_name) %>%
  mutate(name_count = n()) %>%
  ungroup()


ner_df <- ner_df %>%
  left_join(presse %>% select(article_id, source), by = "article_id")



source_stats <- ner_df %>%
  group_by(ent_name, source) %>%
  summarise(name_count = n(), .groups = "drop")

wide <- source_stats %>%
  pivot_wider(
    names_from = source,
    values_from = name_count,
    values_fill = 0,
    names_prefix = "name_count_"
  )


extracted_ents <- ner_df %>%
  group_by(ent_name) %>%
  summarise(
    article_id = first(article_id),
    name_count = first(name_count),
    .groups = "drop"
  ) %>%
  arrange(desc(name_count)) %>%
  left_join(wide, by = "ent_name") %>%
  mutate(name_id = row_number())


write_delim(extracted_ents,
            "/Users/pol/Downloads/extracted_ents_1203.csv",
            delim = ";")

write_delim(ner_df,
            "/Users/pol/Downloads/ner_df_raw.csv",
            delim = ";")



press_corpus_ents <- ner_df %>%
  group_by(article_id) %>%
  summarise(name = list(ent_name), .groups = "drop") %>%
  left_join(presse %>% select(article_id, article_text), by = "article_id") %>%
  select(article_id, article_text, name)

write_delim(press_corpus_ents,
            "/Users/pol/Downloads/press_corpus_ents_1103.csv",
            delim = ";")

# ------------------------------------------------

run_ner_pipeline <- function() {
  cmd <- "python3"  # or full path if needed
  
  script <- "python_press/ner_on_press.py"
  
  result <- system2(
    command = cmd,
    args = script,
    stdout = TRUE,
    stderr = TRUE
  )
  
  status <- attr(result, "status")
  
  if (!is.null(status) && status != 0) {
    stop("Python script failed:\n", paste(result, collapse = "\n"))
  }
  
  return(invisible(result))
}

run_ner_pipeline()

































