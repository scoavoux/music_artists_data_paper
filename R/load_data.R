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

s3 <- initialize_s3()

obj <- s3$get_object(Bucket = "scoavoux", 
                     Key = "records_w3/artists_genre_weight.csv") # load object from s3 bucket

##### load csv and parquet files
load_file <- function(path, file, bucket = "scoavoux", ...) {
  
  s3 <- initialize_s3()
  
  if(grepl("\\.csv", file)) {
    key <- file.path(path, file)
    obj <- s3$get_object(Bucket = bucket, Key = key) # load object from s3 bucket
    txt <- rawToChar(obj$Body) # raw bytes to UTF-8 text
    dat <- data.table::fread(input = txt, nrows = 0, ...) # fread reads strings directly if they contain CSV content
    return(names(dat))
  }
  

  if(grepl("\\.parquet", file)) {
    
    key <- file.path(path, file)
    obj <- s3$get_object(Bucket = bucket, Key = key)
    dat <- arrow::read_parquet(obj$Body, as_data_frame = F)$schema # arrow reads parquet directly from raw vector
    return(names(dat))
  }
  
  #message(cat(file, "loaded"))
  #message(cat("N rows:", nrow(dat)))
  #print(head(dat))
  
  stop("Unsupported file type: ", file)
  
  
}

##### load partitioned dataset
##### for streams
load_partitioned_data <- function(
    bucket = "scoavoux",
    prefix = NULL,
    endpoint = "minio.lab.sspcloud.fr",
    region = Sys.getenv("AWS_DEFAULT_REGION"),
    partition_cols = list(
      REGION = arrow::utf8()
    ),
    cols = NULL
) {
  # ---- Build s3_bucket() source ----
  s3_src <- arrow::s3_bucket(
    bucket,
    endpoint_override = endpoint,
    region = region
  )
  
  # point to the folder inside the bucket
  dat_path <- s3_src$path(prefix)
  
  # ---- Partitioning (optional) ----
  if (is.null(partition_cols)) {
    ds <- arrow::open_dataset(dat_path)
  } else {
    schema <- arrow::schema(!!!partition_cols)
    dat <- arrow::open_dataset(dat_path, partitioning = schema)
  }
  
  dat <- dat %>% 
    select(is_listened) %>% 
    ungroup() %>% 
    collect()
  
  return(dat)
}


# temporary: for stuff stored locally

load_data_file <- function(path="data/", file) {
  file <- paste0(path, file)
  dat <- data.table::fread(input = file, nrows = 0) # fread reads strings directly if they contain CSV content
  return(names(dat))
}


















