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
library(tidyverse)
library(janitor)
library(targets)
library(aws.s3)

## Full dataset run ------
tar_source()
tar_load(all_final)
### If there was a first run
gender_gpt_annotation_path = "gpt_music_data/gpt_gender.csv"
gndr_gpt <- load_s3(gender_gpt_annotation_path, simulation = FALSE) %>% 
  select(dz_artist_id = "artist_id")

df <- df %>% 
  anti_join(gndr_gpt) %>% 
  filter(is.na(gender))

to_code <- select(df, 
                  artist_id = "dz_artist_id", 
                  name = "dz_name", 
                  genre = "genre_1")

full_set_annotation <- annotate_gender(to_code, assistant_path = "data_production/assistant/gpt_gender_assistant.txt")

full_set_annotation <- read_gender_annotations()

gndr_gpt %>% 
  bind_rows(full_set_annotation) %>% 
  add_count(artist_id) %>% 
  filter(n==1) %>% 
  select(-n) %>% 
  write_csv("data/gpt_gender.csv")
