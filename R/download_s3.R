download_s3 <- function(bucket = "scoavoux", key, local = NULL) {
  s3 <- initialize_s3()
  
  if (is.null(local)) {
    # Load file contents into memory (csv, json, txt, small files)
    obj <- s3$get_object(Bucket = bucket, Key = key)
    return(obj$Body)
    
  } else {
    # Download file to disk (parquet, large files)
    s3$download_file(
      Bucket = bucket,
      Key = key,
      Filename = local
    )
    return(local)
  }
}
