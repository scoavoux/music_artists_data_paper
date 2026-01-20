library(tidyverse)
library(httr)
library(lubridate)

## Functions ------
get_album_list <- function(data){
  remove_nulls <- function(.y){
    .y[map(.y, ~!is.null(.x)) |> unlist()]
  }
  
  res <- map(data, remove_nulls) |> 
    map(as.data.frame) |> 
    bind_rows() |> 
    tibble() |> 
    select(album_id = "id", album_title = "title",
           genre_id, record_type, fans, release_date)
  res
  return(res)
}

artists <- read_csv("artists_csv") |> 
  select(artist_id) |> 
  mutate(scrapped = FALSE)
albums <- vector("list", length = nrow(artists))

# time control (limite API: 50 requêtes toutes les 5 secondes
start <- now()
j <- 0
for(i in 1:nrow(artists)){
  if(artists$scrapped[i]) next
  artist_id <- artists$artist_id[i]

  base_url <- str_glue("https://api.deezer.com/artist/{artist_id}/albums")
  page <- RETRY("GET", base_url) |> 
    content()
  j <- j+1
  if(page$total == 0){
    artists$scrapped[i] <- TRUE
    next
  }
  nb_albums <- (page$total %/%25)+1
  res <- vector("list", nb_albums)
  res[[1]] <- get_album_list(page$data)
  a <- 2
  while(!is.null(page$`next`)){
    page <- RETRY("GET", page$`next`) |> 
      content()
    
    res[[a]] <- get_album_list(page$data)
    j <- j + 1 
    a <- a + 1    
  }
  albums[[i]] <- bind_rows(res) |> mutate(artist_id = artists$artist_id[i])
  artists$scrapped[i] <- TRUE
  
  # time control (limite API: 50 requêtes toutes les 5 secondes
  if(j %% 50 == 0) {
    while(now() < (start+duration(6))){
      Sys.sleep(.1)
    }
    print(str_glue("{i} artists done, {nrow(artists)-i} to go"))
    start <- now()
  }
}
al <- bind_rows(albums)
write_csv(al, "2025-12-19_albums.csv")
w <- tibble(record_type = c("album", "compile", "ep", "single"), w = c(1,1,.5,.1))

# Get genre name from API
genres <- distinct(al, genre_id) |> 
  filter(genre_id >= 0) |> 
  mutate(genre = NA)
for(i in 1:nrow(genres)){
  page <- GET(str_glue("https://api.deezer.com/genre/{genres$genre_id[i]}")) |> 
    content()
  if(page$id == genres$genre_id[i]){
    genres$genre[i] <- page$name
  }
}

artists_genre <- al |> 
  left_join(w) |> 
  group_by(artist_id, genre_id) |> 
  summarize(n = sum(w)) |> 
  left_join(genres) |> 
  ungroup()
write.csv(artists_genre, "artists_genre.csv")
