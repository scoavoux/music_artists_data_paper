validate_gender_annotation <- function(
    gender_expert_annotation_path,
    gender_gpt_annotation_path
) {
  
  # Load annotations
  gndr_expert <- load_s3(gender_expert_annotation_path)
  gndr_gpt <- load_s3(gender_gpt_annotation_path)
  
  # Merge
  df <- gndr_gpt %>%
    rename(dz_artist_id = artist_id) %>%
    right_join(gndr_expert, by = "dz_artist_id")
  
  # Match factor levels
  lvls <- union(
    levels(factor(df$gender_expert_sc)),
    levels(factor(df$gender))
  )
  
  df <- df %>%
    mutate(
      popularity_bin   = factor(popularity_bin, levels = unique(gndr_expert$popularity_bin)),
      gender           = factor(gender, levels = lvls),
      gender_expert_sc = factor(gender_expert_sc, levels = lvls)
    )
  
  metrics <- metric_set(precision, recall, f_meas)
  
  results <- df %>%
    filter(gender != "uncertain") %>%
    group_by(popularity_bin) %>%
    metrics(
      truth = gender_expert_sc,
      estimate = gender,
      estimator = "macro_weighted"
    ) %>%
    pivot_wider(names_from = .metric, values_from = .estimate)
  
  table_out <- df %>%
    filter(gender != "uncertain") %>%
    count(popularity_bin, name = "n_certain") %>%
    left_join(results, by = "popularity_bin") %>%
    relocate(n_certain, .after = f_meas)
  
  output_file <- "data/interim/output/gender_validation_metric.tex"
  
  kbl(table_out, format = "latex", digits = 2, booktabs = TRUE) %>%
    save_kable(output_file)
  
  invisible(output_file)
}
