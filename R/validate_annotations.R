validate_annotation <- function(gender_expert_annotation_path = "gpt_music_data/gender_sample_expert_annotated.csv",
                                gender_gpt_annotation_path = "gpt_music_data/gpt_gender.csv"){
  
  library(yardstick)
  library(kableExtra)
  # import expert annotations
  gndr_expert <- load_s3(gender_expert_annotation_path)
  # import llm annotations
  gndr_gpt <- load_s3(gender_gpt_annotation_path)
  
  df <- gndr_gpt %>% 
    rename(dz_artist_id = "artist_id") %>% 
    right_join(gndr_expert, by = "dz_artist_id")
  
  # Ensure matching factor levels; set the level you consider "positive" first.
  lvls <- union(levels(factor(df$gender_expert_sc)), levels(factor(df$gender)))
  
  df <- df %>%
    mutate(
      popularity_bin   = factor(popularity_bin,   levels = unique(gndr_expert$popularity_bin)),
      gender           = factor(gender,           levels = lvls),
      gender_expert_sc = factor(gender_expert_sc, levels = lvls)
    )
  # inspect disagreements
  df %>%
    filter(gender != gender_expert_sc, gender != "uncertain") %>%
    select(dz_artist_id, dz_name, gender, gender_expert_sc)
  
  metrics <- metric_set(precision, recall, f_meas)
  
  results <- df %>%
    filter(gender != "uncertain") %>% 
    group_by(popularity_bin) %>%
    metrics(truth = gender_expert_sc, estimate = gender, estimator = "macro_weighted") %>%
    ungroup()
  
  results_wide <- results %>% 
    pivot_wider(names_from = .metric, values_from = .estimate)
  tb <- df %>%
    filter(gender != "uncertain") %>% 
    group_by(popularity_bin) %>% 
    summarize(n_certain = n()) %>% 
    ungroup() %>% 
    right_join(results_wide, by = "popularity_bin") %>% 
    relocate(n_certain, .after = f_meas)
  
  if(!dir.exists("output")) dir.create("output")
  filename = "output/gender_validation_metric.tex"
  kbl(tb, format = "latex", digits = 2, booktabs = TRUE) %>% 
    save_kable(file = filename)
  return(filename)
  
}