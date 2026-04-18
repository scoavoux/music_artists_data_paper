library(httr2)
library(dplyr)
library(purrr)
library(arrow)

# Config ----
BASE_URL   <- "https://api.deezer.com"
BATCH_SIZE <- 1000
OUT_DIR    <- "data_production/deezer_albums"
MAX_REQ    <- 50
WINDOW_SEC <- 5
dir.create(OUT_DIR, showWarnings = FALSE)

# Rate limiter (sliding window) ----
req_times <- numeric(0)
throttle <- function() {
  now <- Sys.time()
  req_times <<- req_times[as.numeric(now - req_times, units = "secs") < WINDOW_SEC]
  if (length(req_times) >= MAX_REQ) {
    wait <- WINDOW_SEC - as.numeric(now - req_times[1], units = "secs") + 0.05
    if (wait > 0) Sys.sleep(wait)
    req_times <<- req_times[as.numeric(Sys.time() - req_times, units = "secs") < WINDOW_SEC]
  }
  req_times <<- c(req_times, as.numeric(Sys.time()))
}

# Single request with backoff ----
fetch_url <- function(url, max_tries = 6) {
  for (attempt in seq_len(max_tries)) {
    throttle()
    resp <- tryCatch(
      req_perform(req_timeout(request(url), 30)),
      error = function(e) e
    )
    if (inherits(resp, "error")) {
      Sys.sleep(2 ^ attempt); next
    }
    body <- resp_body_json(resp)
    if (!is.null(body$error)) {
      if (isTRUE(body$error$code == 4)) {   # Quota exceeded
        Sys.sleep(2 ^ attempt); next
      }
      return(list(error = body$error))      # non-retryable
    }
    return(body)
  }
  list(error = list(message = "max retries exceeded"))
}

# Extract fields from one album entry ----
extract_album <- function(a) {
  tibble(
    album_id     = a$id,
    album_title  = if (is.null(a$title))        NA_character_ else a$title,
    genre_id     = if (is.null(a$genre_id))     NA_integer_   else a$genre_id,
    record_type  = if (is.null(a$record_type))  NA_character_ else a$record_type,
    fans         = if (is.null(a$fans))         NA_integer_   else a$fans,
    release_date = if (is.null(a$release_date)) NA_character_ else a$release_date
  )
}

# Get all albums for one artist ----
get_artist_albums <- function(artist_id) {
  url <- sprintf("%s/artist/%s/albums?limit=25&index=0", BASE_URL, artist_id)
  pages <- list()
  repeat {
    body <- fetch_url(url)
    if (!is.null(body$error)) {
      msg <- if (is.null(body$error$message)) "unknown" else body$error$message
      return(tibble(artist_id = artist_id, error = msg))
    }
    if (length(body$data) == 0) break
    pages[[length(pages) + 1]] <- body$data
    if (is.null(body$`next`)) break
    url <- body$`next`
  }
  if (length(pages) == 0) {
    return(tibble(artist_id = artist_id, error = NA_character_))
  }
  albums <- map_dfr(pages, function(p) map_dfr(p, extract_album))
  albums$artist_id <- artist_id
  albums$error     <- NA_character_
  albums
}

# Main loop ----
run_scrape <- function(artist_ids) {
  done_files <- list.files(OUT_DIR, pattern = "\\.parquet$", full.names = TRUE)
  done_ids <- if (length(done_files)) {
    unique(unlist(lapply(done_files, function(f) read_parquet(f)$artist_id)))
  } else integer(0)
  todo <- setdiff(artist_ids, done_ids)
  message(sprintf("Resuming: %d done, %d to do", length(done_ids), length(todo)))
  
  buffer    <- vector("list", BATCH_SIZE)
  buf_i     <- 0L
  batch_num <- length(done_files) + 1L
  
  flush <- function() {
    if (buf_i == 0L) return()
    out <- bind_rows(buffer[seq_len(buf_i)])
    write_parquet(out, file.path(OUT_DIR, sprintf("batch_%05d.parquet", batch_num)))
    message(sprintf("[%s] wrote batch %d (%d artists, %d rows)",
                    Sys.time(), batch_num, buf_i, nrow(out)))
    batch_num <<- batch_num + 1L
    buffer    <<- vector("list", BATCH_SIZE)
    buf_i     <<- 0L
  }
  
  for (i in seq_along(todo)) {
    aid <- todo[i]
    res <- tryCatch(
      get_artist_albums(aid),
      error = function(e) tibble(artist_id = aid, error = conditionMessage(e))
    )
    buf_i <- buf_i + 1L
    buffer[[buf_i]] <- res
    if (buf_i >= BATCH_SIZE) flush()
  }
  flush()
}

# Scraping ------
tar_load(all_final_press)

artists <- all_final_press |> 
  select(artist_id = dz_artist_id) %>% 
  distinct
run_scrape(artists$artist_id)

# Results ------
load_results <- function(dir = OUT_DIR) {
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)
  if (length(files) == 0) {
    warning("No parquet files found in ", dir)
    return(tibble())
  }
  bind_rows(lapply(files, read_parquet))
}

all_albums <- load_results() %>% 
  select(-error)

