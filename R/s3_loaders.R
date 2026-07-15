### functions to load data

##### initialize s3
initialize_s3 <- function(){
  
    s3 <- paws::s3(config = list(
    credentials = list(
      creds = list(
        access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
        session_token = Sys.getenv("AWS_SESSION_TOKEN")
      )),
    endpoint = paste0("https://", Sys.getenv("AWS_S3_ENDPOINT")),
    region = Sys.getenv("AWS_DEFAULT_REGION")))
  
  return(s3)
}


# load files either from s3 or locally
load_s3 <- function(file,
                    bucket = "scoavoux",
                    simulation = SIMULATION,
                    path = LOCAL_DATA_DIR,
                    ...) {
  
  # -----------------------------
  # LOCAL SIMULATION MODE
  # -----------------------------
  if (simulation) {
    
    if (grepl("\\.csv$", file)) {
      
      dat <- data.table::fread(
        paste0(path,file),
        ...
      )
      
      return(dat)
    }
    
    if (grepl("\\.parquet$", file)) {
      
      dat <- arrow::read_parquet(
        paste0(path,file),
        as_data_frame = TRUE
      )
      
      return(dat)
    }
    
    stop("Unsupported file type: ", file)
  }
  
  # -----------------------------
  # S3 PRODUCTION MODE
  # -----------------------------
  s3 <- initialize_s3()
  
  if (grepl("\\.csv$", file)) {
    
    obj <- s3$get_object(
      Bucket = bucket,
      Key = file
    )
    
    txt <- rawToChar(obj$Body)
    
    dat <- data.table::fread(
      input = txt,
      ...
    )
    
    return(dat)
  }
  
  if (grepl("\\.parquet$", file)) {
    
    obj <- s3$get_object(
      Bucket = bucket,
      Key = file
    )
    
    buf <- arrow::BufferReader$create(obj$Body)
    
    dat <- arrow::read_parquet(
      buf,
      as_data_frame = TRUE
    )
    
    return(dat)
  }
  
  stop("Unsupported file type: ", file)
}



### export data
write_s3 <- function(x,
                     file,
                     simulation = SIMULATION,
                     local_data_dir = LOCAL_DATA_DIR) {
  
  # -----------------------------
  # LOCAL SIMULATION MODE
  # -----------------------------
  if (simulation) {
    
    local_file <- file.path(local_data_dir, file)
    
    dir.create(
      dirname(local_file),
      recursive = TRUE,
      showWarnings = FALSE
    )
    
    if (grepl("\\.csv$", file)) {
      
      readr::write_csv(x, local_file)
      
      return(invisible(local_file))
    }
    
    if (grepl("\\.parquet$", file)) {
      
      arrow::write_parquet(x, local_file)
      
      return(invisible(local_file))
    }
    
    stop("Unsupported file type: ", file)
  }
  
  # -----------------------------
  # S3 PRODUCTION MODE
  # -----------------------------
  s3 <- initialize_s3()
  
  tmp <- tempfile(
    fileext = paste0(".", tools::file_ext(file))
  )
  
  if (grepl("\\.csv$", file)) {
    
    readr::write_csv(x, tmp)
    
  } else if (grepl("\\.parquet$", file)) {
    
    arrow::write_parquet(x, tmp)
    
  } else {
    
    stop("Unsupported file type: ", file)
    
  }
  
  s3$put_object(
    Body = tmp,
    Bucket = "scoavoux",
    Key = file
  )
  
  invisible(file)
}

# download entire folder from s3
# delete later, only used to reorganize stuff
download_s3_folder <- function(
    prefix,
    local_dir,
    bucket = "scoavoux"
) {
  
  s3 <- initialize_s3()
  
  # Create local directory if needed
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)
  
  # List all objects under the prefix
  res <- s3$list_objects_v2(
    Bucket = bucket,
    Prefix = prefix
  )
  
  if (is.null(res$Contents) || length(res$Contents) == 0) {
    stop("No files found under prefix: ", prefix)
  }
  
  for (obj in res$Contents) {
    
    key <- obj$Key
    
    # Skip folder placeholders
    if (grepl("/$", key)) next
    
    local_file <- file.path(
      local_dir,
      basename(key)
    )
    
    message("Downloading: ", key)
    
    file_obj <- s3$get_object(
      Bucket = bucket,
      Key = key
    )
    
    writeBin(
      object = file_obj$Body,
      con = local_file
    )
  }
  
  message("Done.")
}

# duckdb initialization
duckdb_s3_con <- function() {
  
  con <- DBI::dbConnect(duckdb::duckdb())
  
  # enable direct reads from S3/http
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  
  # MinIO is path-style and served over https://<AWS_S3_ENDPOINT>
  DBI::dbExecute(con, "SET s3_url_style = 'path';")
  DBI::dbExecute(con, "SET s3_use_ssl   = true;")
  
  DBI::dbExecute(con, glue::glue_sql(
    "SET s3_endpoint = {Sys.getenv('AWS_S3_ENDPOINT')};", .con = con))
  DBI::dbExecute(con, glue::glue_sql(
    "SET s3_region = {Sys.getenv('AWS_DEFAULT_REGION')};", .con = con))
  DBI::dbExecute(con, glue::glue_sql(
    "SET s3_access_key_id = {Sys.getenv('AWS_ACCESS_KEY_ID')};", .con = con))
  DBI::dbExecute(con, glue::glue_sql(
    "SET s3_secret_access_key = {Sys.getenv('AWS_SECRET_ACCESS_KEY')};", .con = con))
  
  # temporary creds (paws also uses AWS_SESSION_TOKEN); set only if present
  if (nzchar(Sys.getenv("AWS_SESSION_TOKEN"))) {
    DBI::dbExecute(con, glue::glue_sql(
      "SET s3_session_token = {Sys.getenv('AWS_SESSION_TOKEN')};", .con = con))
  }
  
  con
}













