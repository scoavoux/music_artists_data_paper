# =============================================================================
# PCS-ISEI profession recoding pipeline, end to end.
#
# Two entry points, around the interactive coding step (pcs_isei_app.R):
#
#   source("pipeline.R")
#   step1_prepare_candidates()                       # -> candidates.csv, labels_pcs.csv
#   #   ... open app.R, code the candidates, download annotations_*.csv ...
#   step2_finalize("annotations_20260604_231346_finales.csv")   # -> professions_recodees.csv
# =============================================================================

library(tidyverse)
library(readxl)
library(Matrix)

# ---- configuration ----------------------------------------------------------
SURVEY_CSV     <- "RECORDS_Wave3_apr_june_23_responses_corrected.csv"
LABELS_URL     <- "https://www.nomenclature-pcs.fr/uploads/L61_Liste_libell%C3%A9s_professions_hommes_et_femmes_collecte2026.xlsx"
LABELS_XLSX    <- "labels_officiels.xlsx"   # local cache of the download
CANDIDATES_CSV <- "candidates.csv"          # app.R input: non-exact responses, top-k labels
LABELS_APP_CSV <- "labels_pcs.csv"          # app.R input: full label list (lowercased)
MATCHES_RDS    <- "matches.rds"             # handoff between step 1 and step 2
OUTPUT_CSV     <- "professions_recodees.csv"
TOP_K          <- 20

# ---- labels: download + parse ----------------------------------------------
# Official list: columns id (= code), LIBELLE_MASCULIN, LIBELLE_FEMININ, with a
# leading "collection year" marker row (libelle == "2026") that we drop.
load_labels <- function() {
  if (!file.exists(LABELS_XLSX)) download.file(LABELS_URL, LABELS_XLSX, mode = "wb")
  read_excel(LABELS_XLSX) |>
    rename(code = id, libelle_m = LIBELLE_MASCULIN, libelle_f = LIBELLE_FEMININ) |>
    filter(!str_detect(libelle_m, "^[0-9]{4}$")) |>   # drop the year-marker row
    mutate(code = as.integer(code))
}

# Long form, one row per (code, gender), keeping the original casing AND its
# lowercase counterpart — this link is what removes the final casing merge.
labels_to_long <- function(labels) {
  labels |>
    pivot_longer(c(libelle_m, libelle_f), names_to = "genre", values_to = "libelle") |>
    filter(!is.na(libelle)) |>
    mutate(libelle_lc = str_to_lower(libelle), label_id = row_number())
}

# ---- survey professions -----------------------------------------------------
# One row per declared profession (original survey casing) plus its lowercase
# key. Replaces prepare_for_max_app.R (the STATUT/PUB/... variables it also
# computed are not used for recoding, so they are dropped here).
load_survey_professions <- function() {
  read_csv(SURVEY_CSV, show_col_types = FALSE) |>
    select(hashed_id, E_FR_prof_femme:E_FR_prof_retr_homme) |>
    pivot_longer(E_FR_prof_femme:E_FR_prof_retr_homme) |>
    filter(!is.na(value)) |>
    transmute(hashed_id,
              profession_declaree = value,
              reponse = str_to_lower(value))   # the key linking answer <-> matching
}

# ---- character n-gram TF-IDF matcher ---------------------------------------
char_ngrams <- function(text, n_min = 2, n_max = 4) {
  text <- paste0(" ", tolower(trimws(text)), " ")
  out <- character(0)
  for (n in n_min:n_max)
    if (nchar(text) >= n)
      out <- c(out, substring(text, 1:(nchar(text) - n + 1), n:nchar(text)))
  out
}

# Match a vector of (unique) responses against the labels.
# Returns: reponse, rank, code, libelle (original), libelle_lc, similarity.
match_responses <- function(responses, lab_long) {
  label_ngrams <- tibble(label_id = lab_long$label_id, text = lab_long$libelle_lc) |>
    mutate(ngrams = map(text, char_ngrams)) |>
    unnest(ngrams) |>
    count(label_id, ngrams)

  n_docs <- nrow(lab_long)
  idf <- label_ngrams |>
    distinct(label_id, ngrams) |>
    count(ngrams, name = "df") |>
    mutate(idf_value = log(n_docs / df), ngram_id = row_number()) |>
    select(ngrams, ngram_id, idf_value)

  label_tfidf <- label_ngrams |>
    inner_join(idf, by = "ngrams") |>
    mutate(tfidf = n * idf_value)

  label_mat <- sparseMatrix(i = label_tfidf$label_id, j = label_tfidf$ngram_id,
                            x = label_tfidf$tfidf, dims = c(n_docs, nrow(idf)))
  norms <- sqrt(rowSums(label_mat^2)); norms[norms == 0] <- 1
  label_mat <- label_mat / norms                      # L2-normalize label rows

  match_one <- function(txt) {
    ng <- table(char_ngrams(txt))
    ng_df <- tibble(ngrams = names(ng), tf = as.numeric(ng)) |>
      inner_join(idf, by = "ngrams") |>
      mutate(tfidf = tf * idf_value)
    if (nrow(ng_df) == 0) return(tibble())
    q <- sparseMatrix(i = rep(1, nrow(ng_df)), j = ng_df$ngram_id, x = ng_df$tfidf,
                      dims = c(1, nrow(idf)))
    qn <- sqrt(sum(q^2)); if (qn == 0) return(tibble())
    sims <- as.numeric((q / qn) %*% t(label_mat))     # cosine similarity
    idx <- order(sims, decreasing = TRUE)[1:TOP_K]
    tibble(label_id = idx, similarity = round(sims[idx], 3))
  }

  tibble(reponse = responses) |>
    mutate(m = map(reponse, match_one)) |>
    unnest(m) |>
    left_join(select(lab_long, label_id, code, libelle, libelle_lc), by = "label_id") |>
    group_by(reponse, code) |>                        # collapse gendered variants
    slice_max(similarity, n = 1, with_ties = FALSE) |>
    ungroup() |>
    group_by(reponse) |>
    mutate(rank = row_number(desc(similarity))) |>
    filter(rank <= TOP_K) |>
    ungroup() |>
    select(reponse, rank, code, libelle, libelle_lc, similarity) |>
    arrange(reponse, rank)
}

# ---- step 1: prepare candidates for the manual coding app -------------------
step1_prepare_candidates <- function() {
  labels   <- load_labels()
  lab_long <- labels_to_long(labels)

  # app.R search box needs the full label list (lowercased), columns unchanged
  labels |>
    transmute(code,
              libelle_m = str_to_lower(libelle_m),
              libelle_f = str_to_lower(libelle_f)) |>
    write_csv(LABELS_APP_CSV)

  survey  <- load_survey_professions()
  matches <- match_responses(unique(survey$reponse), lab_long)
  saveRDS(matches, MATCHES_RDS)                        # central object reused in step 2

  # exact matches (similarity == 1) are auto-coded; the rest need manual coding
  exact_resp <- matches |> filter(similarity == 1) |> pull(reponse) |> unique()

  candidates <- matches |> filter(!reponse %in% exact_resp)
  cand_id    <- tibble(reponse = unique(candidates$reponse)) |> mutate(id = row_number())

  candidates |>
    left_join(cand_id, by = "reponse") |>
    transmute(id, reponse, rank, code, libelle = libelle_lc, similarity) |>
    arrange(id, rank) |>
    write_csv(CANDIDATES_CSV)

  message(nrow(survey), " declared professions | ",
          length(unique(survey$reponse)), " unique | ",
          length(exact_resp), " exact, ", nrow(cand_id), " to code by hand")
  invisible(matches)
}

# ---- step 2: import manual codes and produce the final dataset --------------
step2_finalize <- function(annotations_csv) {
  lab_long <- labels_to_long(load_labels())
  casing   <- distinct(lab_long, libelle_lc, libelle)   # lowercase -> original (1:1)
  matches  <- readRDS(MATCHES_RDS)

  # exact matches: recode = the matched label itself, in original casing
  exact <- matches |>
    filter(similarity == 1) |>
    group_by(reponse) |> slice_min(rank, n = 1, with_ties = FALSE) |> ungroup() |>
    transmute(reponse, profession_recode = libelle)

  # manual codes: chosen_libelle is lowercase (NA = "no match") -> restore casing
  manual <- read_csv(annotations_csv, show_col_types = FALSE) |>
    transmute(reponse, libelle_lc = chosen_libelle) |>
    left_join(casing, by = "libelle_lc") |>
    transmute(reponse, profession_recode = coalesce(libelle, libelle_lc))

  # one recode per unique response: exact wins, otherwise the manual code (or NA)
  recode <- bind_rows(exact, anti_join(manual, exact, by = "reponse")) |>
    distinct(reponse, .keep_all = TRUE)

  out <- load_survey_professions() |>
    left_join(recode, by = "reponse") |>
    select(hashed_id, profession_declaree, profession_recode)

  write_csv(out, OUTPUT_CSV)
  message(nrow(out), " rows written to ", OUTPUT_CSV, " | ",
          sum(!is.na(out$profession_recode)), " recoded, ",
          sum(is.na(out$profession_recode)), " NA")
  invisible(out)
}

