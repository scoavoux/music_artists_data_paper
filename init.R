
# import all packages listed by renv at once
import_all_pkg <- function(packages){
  
  needed_packages <- unique(packages)
  
  for(i in 1:length(needed_packages)){
    
    library(needed_packages[i], character.only = TRUE)
    
  }
  
  
}

# special installation for this which is not on CRAN anymore
# install.packages("WikidataQueryServiceR", repos = c(
#   "https://wikimedia.r-universe.dev",
#   "https://cloud.r-project.org"
# ))



# set theme and pane layout 

rstudioapi::applyTheme("Merbivore Soft")

rstudioapi::writeRStudioPreference("pane_config", list(
     console = "left",
     source = "right",
     tabSet1 = "right",
     tabSet2 = "right",
     hiddenTabSet = "none"
     ))
















