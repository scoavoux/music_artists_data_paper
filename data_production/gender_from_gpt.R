# Functions ------
annotate_gender <- function(artists, assistant_path, output_dir = "gpt_batches", batch_size = 50) {
  library(tidyverse)
  library(openai)
  library(jsonlite)
  
  assistant_prompt <- read_file(assistant_path)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  batches <- split(artists, ceiling(seq_len(nrow(artists)) / batch_size))
  
  results <- list()
  
  for (i in seq_along(batches)) {
    batch_file <- file.path(output_dir, sprintf("batch_%03d.json", i))
    
    if (file.exists(batch_file)) {
      message(sprintf("Batch %d: already exists, skipping", i))
      saved <- fromJSON(batch_file)
      results[[i]] <- as_tibble(saved$parsed)
      next
    }
    
    batch <- batches[[i]]
    input_json <- toJSON(
      batch |> select(id = "artist_id", name, genre),
      auto_unbox = TRUE
    )
    
    tryCatch({
      response <- create_chat_completion(
        model = "gpt-5-mini",
        messages = list(
          list(role = "system", content = assistant_prompt),
          list(role = "user", content = input_json)
        )
      )
      
      raw_content <- response$choices$message.content[1]
      parsed <- fromJSON(raw_content)
      
      parsed <- as_tibble(parsed) |>
        rename(artist_id = id)
      
      batch_out <- list(
        batch_index = i,
        n_artists = nrow(batch),
        raw_response = raw_content,
        parsed = parsed
      )
      write_json(batch_out, batch_file, auto_unbox = TRUE)
      
      results[[i]] <- parsed
      message(sprintf("Batch %d/%d: OK (%d artists)", i, length(batches), nrow(parsed)))
      
    }, error = function(e) {
      message(sprintf("Batch %d/%d: ERROR - %s", i, length(batches), conditionMessage(e)))
    })
  }
  
  results <- bind_rows(results)
  return(results)
}

read_gender_annotations <- function(output_dir = "gpt_batches") {
  library(tidyverse)
  library(jsonlite)
  
  files <- list.files(output_dir, pattern = "^batch_.*\\.json$", full.names = TRUE)
  if (length(files) == 0) {
    warning("No batch files found in ", output_dir)
    return(tibble(artist_id = character(), name = character(), gender = character()))
  }
  
  map(files, \(f) {
    saved <- fromJSON(f)
    as_tibble(saved$parsed) |> 
      mutate(artist_id = as.integer(artist_id))
  }) |>
    bind_rows() |>
    select(artist_id, name, gender)
}

# Run ------
## Test set ------
library(tidyverse)
library(janitor)
manual_annotation <- read_csv("data/artistes_200_test - artists_200_randomsample.csv")
test_set_annotation <- annotate_gender(manual_annotation, assistant_path = "assistant/gpt_gender_assistant.txt")
test_set_annotation <- test_set_annotation |> 
  rename(gpt_gender = "gender") %>% 
  left_join(select(manual_annotation, artist_id, human_annotation, genre)) |> 
  mutate(human_annotation = factor(human_annotation, levels = c(1, 2), labels = c("male", "female")))
tabyl(test_set_annotation, gpt_gender, human_annotation)

## Full dataset run ------
tar_load(artists)
to_code <- select(artists, artist_id, name, genre)
full_set_annotation <- annotate_gender(to_code, assistant_path = "assistant/gpt_gender_assistant.txt")

#full_set_annotation <- read_gender_annotations()
full_set_annotation <- full_set_annotation %>% 
  select(artist_id, gpt_gender = "gender") %>% 
  # Should rather be saved on s3
  write_csv("data/gpt_gender.csv")
