args <- commandArgs(trailingOnly = TRUE)

file_path <- args[1]
file_name <- paste0("musicbrainz/", stringr::str_extract(file_path, "[^/]+\\.csv"))

aws.s3::put_object(file = file_path, object = file_name, bucket = "scoavoux", region = "")
