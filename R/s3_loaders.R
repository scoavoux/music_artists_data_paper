### functions to load data

##### initialize s3
initialize_s3 <- function(){
  require("paws")
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
                    ...) {
  
  # -----------------------------
  # LOCAL SIMULATION MODE
  # -----------------------------
  if (simulation) {
    
    if (grepl("\\.csv$", file)) {
      
      dat <- data.table::fread(
        file,
        ...
      )
      
      return(dat)
    }
    
    if (grepl("\\.parquet$", file)) {
      
      dat <- arrow::read_parquet(
        file,
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
### export data
write_s3 <- function(x, file) {
  
  require(arrow)
  
  s3 <- initialize_s3()
  
  tmp <- tempfile(fileext = tools::file_ext(file))
  
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















