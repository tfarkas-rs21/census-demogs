library(dplyr)
library(purrr)
library(cubelyr)

# load required data
load("~/projects/mothr/mobility/census-demogs/data/pums_hhld.RData")
load("~/projects/mothr/mobility/census-demogs/data/pums_prsn.RData")
load("~/projects/mothr/mobility/census-demogs/data/tract_tables_list.RData")
load("~/projects/mothr/mobility/census-demogs/data/tract_puma_mapping.RData")
source("~/projects/mothr/mobility/census-demogs/helper-functions.R")


### an alternate approach for parallelization

pums_hh_names <- pums_hh %>%
  distinct(ST, PUMA) %>%
  mutate(puma_id = paste0(ST,PUMA), 
         hh_ind = TRUE)

pums_prsn_names <- pums_prsn_df %>%
  distinct(state, puma) %>%
  mutate(puma_id = paste0(state,puma), 
         prsn_ind = TRUE)

tract_puma_df_inds <- tract_puma_df %>%
  left_join(pums_hh_names %>% select(puma_id, hh_ind)) %>%
  left_join(pums_prsn_names %>% select(puma_id, prsn_ind)) %>%
  mutate(across(ends_with("ind"), ~ ifelse(is.na(.x), FALSE, .x)), 
         puma_hh = ifelse(hh_ind, puma_id, "ones"), 
         puma_prsn = ifelse(prsn_ind, puma_id, "ones"))

## split household and person pums into lists, add a "ones" element, and name them
# houshold file
pums_hh_list <- pums_hh %>%
  group_by(ST, PUMA) %>%
  group_split %>%
  set_names(pums_hh %>% 
              distinct(ST, PUMA) %>%
              mutate(geoid = paste0(ST, PUMA)) %>%
              pull(geoid)) %>% as.list

na_puma_hh <- pums_hh_list[[1]] %>%
  mutate(N_HSHLD = 1)

pums_hh_list <- c(pums_hh_list, list("ones" = na_puma_hh))

# person file
pums_prsn_list <- pums_prsn_df %>%
  group_by(state, puma) %>%
  group_split %>%
  set_names(pums_prsn_df %>% 
              distinct(state, puma) %>%
              mutate(geoid = paste0(state, puma)) %>%
              pull(geoid)) %>% as.list

na_puma_prsn <- pums_prsn_list[[1]] %>%
  mutate(N_HSHLD = 1)

pums_prsn_list <- c(pums_prsn_list, list("ones" = na_puma_prsn))

###### use lists to combine and transpose 

tract_sub <- tract_puma_df_inds %>%
  sample_n(4)
tract_names <- tract_sub %>% pull(geoid)

# they all need the same names or transpose yields NULL for mismatches
ts <- tract_list[tract_sub %>% pull(geoid)] %>% set_names(tract_names)
ph <- pums_hh_list[tract_sub %>% pull(puma_hh)] %>% set_names(tract_names)
pp <- pums_prsn_list[tract_sub %>% pull(puma_prsn)] %>% set_names(tract_names)

tract_all_data <- transpose(list(tct = ts, hhd = ph, psn = pp))





######## This approach is too slow!! Can't parallelize!

# make tract list that has PUMA data and identifiers
all_data_list <- map(tract_list[1:4], ~{
  
  #### PUMS to seed IPF 
  thispuma <- .x %>% pull(puma) %>% unique # PUMA for tract
  thisstate <- .x %>% pull(state) %>% unique # state for tract
  
  # create seed for final IPF from enclosing PUMA to this tract
  pums_prsn <- pums_prsn_df %>%
    filter(puma == thispuma , 
           state == thisstate) %>%
    arrange(ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh) %>%
    select(-c(puma, state)) %>%
    tbl_pivot_array
  
  # create seed for intermediate IPF to create person-level data from 
  # household level data, leveraging NP (number of people per household)
  # from raw PUMS data. 
  pums_hhld <- pums_hh %>%
    filter(PUMA == thispuma, 
           ST == thisstate) %>%
    select(NP, income_hh, ethn_hh, age_hh, race_hh, N_HSHLD) %>%
    arrange(NP, income_hh, ethn_hh, age_hh, race_hh) %>%
    tbl_pivot_array(met_name = "N_HSHLD")
  
  list(tract_data = .x, pums_prsn = pums_prsn, pums_hhld = pums_hhld, 
       puma = thispuma, geoid = unique(.x$geoid))
  
})

save(all_data_list, file = "~/projects/mothr/mobility/census-demogs/data/all_data_list.RData")

