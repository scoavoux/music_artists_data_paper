load_s3_parquet <- function(
    path,
    bucket = "scoavoux",
    endpoint = "minio.lab.sspcloud.fr",
    partition_vars = c("REGION")
) {
  require(arrow)
  
  # Build partition schema
  schema_list <- lapply(partition_vars, function(var) arrow::utf8())
  names(schema_list) <- partition_vars
  partition_schema <- do.call(arrow::schema, schema_list)
  
  # Open dataset lazily via Arrow
  message("→ Opening Parquet dataset lazily from S3...")
  data_cloud <- arrow::open_dataset(
    source = arrow::s3_bucket(bucket, endpoint_override = endpoint)$path(path),
    partitioning = partition_schema
  )
  
  return(data_cloud)
}
