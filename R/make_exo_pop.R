make_gfk_pop <- function(dz_songs,
                         simulation = SIMULATION){
  if(simulation){
    return(tibble(dz_artist_id = NA))
  }
  
  # Import  GfK data (file is ISRC/week/sales chanel)
  # Aggregate per year at ISRC level
  library(DBI)
  library(duckdb)
  library(glue)
  
  import_gfk_duckdb <- function(years  = 2019:2022,
                                bucket = "scoavoux",
                                con    = NULL) {
    
    con <- duckdb_s3_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # --- list of parquet keys on S3 --------------------------------------------
    files    <- glue("s3://{bucket}/GfK/{years}.parquet")
    file_lit <- paste0("[", paste(sprintf("'%s'", files), collapse = ", "), "]")
    
    # --- the query --------------------------------------------------------------
    sql <- paste0("
    SELECT
      ean_isrc AS isrc,
      'gfkpop_' || year || '_' || salestype AS lab,
      sales
    FROM (
      SELECT
        regexp_extract(week_code_w::VARCHAR, '^[0-9]{4}') AS year,
        ean_isrc,
        CASE WHEN salestype = 'Premium Streams' THEN 'premium' ELSE 'free' END AS salestype,
        sum(units_panel_w) AS sales
      FROM read_parquet(", file_lit, ")
      WHERE NOT starts_with(ean_isrc, 'XX_')
        AND salestype IN ('Premium Streams', 'free music stream', 'free music tream')
      GROUP BY year, ean_isrc, salestype
    )
  ")
    
    dbGetQuery(con, sql)
  }
  
  gfk <- import_gfk_duckdb(2019:2022)
  distinct_gfk_isrc <- distinct(gfk, isrc)
  
  # From ISRC to track id: pair with song_ids_isrc_matched.csv (WARNING: one ISRC -> many track ids)  
  isrc <- load_s3("records_w3/items/song_ids_isrc_matched.csv")
  isrc_artist_id <- distinct_gfk_isrc %>% 
    inner_join(isrc) %>% 
  # From track_id to artist_id: pair with dz_songs (WARNING: one track_id -> many artists)
    inner_join(select(dz_songs, song_id, dz_artist_id)) %>% 
  # Reduce to ISRC. For each ISRC, one line for each artist tagged in at least one track id
    distinct(isrc, dz_artist_id)
  
  # Summarize at artist level
  res <- gfk %>% 
    inner_join(isrc_artist_id) %>% 
    group_by(dz_artist_id, lab) %>% 
    summarize(sales = sum(sales)) %>% 
    ungroup() %>% 
    pivot_wider(names_from = lab, values_from = sales)
  res
  return(res)
}