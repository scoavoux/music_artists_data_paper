load_s3 <- function(path,
                    bucket = "scoavoux",
                    base_path = "records_w3",
                    endpoint = "minio.lab.sspcloud.fr",
                    partition_vars = c("REGION")) {
  library(arrow)
  library(data.table)
  
  # Construct full S3 path
  s3 <- arrow::s3_bucket(bucket, endpoint_override = endpoint)
  full_path <- file.path(base_path, path)
  
  # Detect file type from extension
  ext <- tools::file_ext(path)
  
  if (ext %in% c("parquet", "snappy")) {
    # Build schema for partitioning
    schema_list <- lapply(partition_vars, function(var) arrow::utf8())
    names(schema_list) <- partition_vars
    partition_schema <- do.call(arrow::schema, schema_list)
    
    message("loading parquet dataset lazily from S3...")
    data <- arrow::open_dataset(
      source = s3$path(full_path),
      partitioning = partition_schema
    )
    
  } else if (ext == "csv") {
    message("loading csv file directly into memory...")
    # Build signed S3 URI
    file_uri <- paste0("s3://", bucket, "/", full_path)
    
    # Use Arrow’s filesystem to create a signed URL
    fs <- arrow::s3_bucket(bucket, endpoint_override = endpoint)
    tmp_file <- tempfile(fileext = ".csv")
    arrow::copy_files(fs$path(full_path), tmp_file)
    
    data <- data.table::fread(tmp_file)
    unlink(tmp_file)
    
  } else {
    stop("Unsupported file type. Supported: .csv, .parquet, .snappy.parquet")
  }
  
  return(data)
}

load_s3 <- function(path,
                    basepath = "scoavoux/records_w3",
                    endpoint = "minio.lab.sspcloud.fr",
                    partition_vars = c("REGION")) {
  library(arrow)
  library(data.table)
  
    file_uri <- paste0("s3://", basepath, "/", path)

    df <- data.table::fread(file_uri)
  
  return(data)
}









































