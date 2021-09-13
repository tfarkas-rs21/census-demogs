library(dplyr)
library(tidyr)
library(purrr)
library(readr)
#library(furrr)
library(parallel)

data_path <- "~/projects/mothr/mobility/census-demogs/data/" # local
#data_path <- "~/hdd/data/" # ec2

## paralellization with furrr
#future::plan(sequential)
#future::plan(multisession)

# houshold file
load(paste0(data_path, "pums_hhld.RData"))

pums_hh_list <- pums_hh %>%
  complete(nesting(ST, PUMA, NP), income_hh, ethn_hh, age_hh, race_hh, 
           fill = list(N_HSHLD = 0)) %>%
  group_by(ST, PUMA) %>%
  group_split %>%
  set_names(pums_hh %>% 
              distinct(ST, PUMA) %>%
              mutate(geoid = paste0(ST, PUMA)) %>%
              pull(geoid)) %>% as.list

na_puma_hh <- pums_hh_list[[1]] %>%
  mutate(N_HSHLD = .1)

pums_hh_list <- c(pums_hh_list, list("ones" = na_puma_hh))

# write_csv each pums_hh out as individual RData files
t0 <- Sys.time()
mcmapply(FUN = function(.x, .y) {
  filename = paste0(data_path, "puma_hh/", "pums_hh_", .y, ".csv")
  print(paste(filename, Sys.time() - t0, sep = ", "))  
#  cat("\n")
  write_csv(.x, file = filename)
     }, 
.x = pums_hh_list[1:10], .y = names(pums_hh_list)[1:10])

rm(pums_hh, pums_hh_list, na_puma_hh)
gc(FALSE)

# person file
load(paste0(data_path, "pums_prsn.RData"))

pums_prsn_list <- pums_prsn_df %>%
  complete(nesting(state, puma), ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh, 
           fill = list(count = 0)) %>%
  group_by(state, puma) %>%
  group_split %>%
  set_names(pums_prsn_df %>% 
              distinct(state, puma) %>%
              mutate(geoid = paste0(state, puma)) %>%
              pull(geoid)) %>% as.list

na_puma_prsn <- pums_prsn_list[[1]] %>%
  mutate(count = .1)

pums_prsn_list <- c(pums_prsn_list, list("ones" = na_puma_prsn))
t0 = Sys.time()
# write_csv each pums_pp out as individual RData files
mcmapply(FUN = function(.x, .y) {
  filename = paste0(data_path, "puma_pp/", "pums_pp_", .y, ".csv")
  print(paste(filename, Sys.time() - t0, sep = ", "))  
#  cat("\n")
  write_csv(.x, file = filename)
	      },
.x = pums_prsn_list[1:10], .y = names(pums_prsn_list)[1:10])

rm(pums_prsn_df, pums_prsn_list, na_puma_prsn)
gc(FALSE)

# tracts 
load(paste0(data_path,"tract_tables_list.RData"))
t0 = Sys.time()
mcmapply(function(.x, .y) {
  filename = paste0(data_path, "tracts/", "tract2_", .y, ".csv")
  print(paste(filename, Sys.time() - t0, sep = ", "))  
#  cat("\n")
  write_csv(.x, file = filename)
}, 
.x = tract_list[1:10], .y = names(tract_list)[1:10])

