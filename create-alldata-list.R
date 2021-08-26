library(dplyr)
library(purrr)
library(cublelyr)

# load required data
load("~/projects/mothr/mobility/census-demogs/data/pums_hhld.RData")
load("~/projects/mothr/mobility/census-demogs/data/pums_prsn.RData")
load("~/projects/mothr/mobility/census-demogs/data/tract_tables_list.RData")
source("~/projects/mothr/mobility/census-demogs/helper-functions.R")

all_data_list = list()

# make tract list that had PUMA data
walk(tract_list[1:4], ~{
  
  #### PUMS to seed IPF 
  thispuma <- .x %>% pull(puma) %>% unique # PUMA for tract
  thisstate <- .x %>% pull(state) %>% unique # state for tract
  
  # create seed for final IPF from enclosing PUMA to this tract
  pums_prsn <- pums_prsn_df %>%
    filter(puma == thispuma , 
           state == thisstate) %>%
    mutate(across(count, ~ ifelse(.x == 0, .1, .x))) %>%
    arrange(ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh) %>%
    select(-c(puma, state)) %>%
    tbl_pivot_array
  
  # create seed for intermediate IPF to create person-level data from 
  # household level data, leveraging NP (number of people per household)
  # from raw PUMS data. 
  pums_hhld <- pums_hh %>%
    filter(PUMA == thispuma, 
           ST == thisstate) %>%
    mutate(across(N_HSHLD, ~ ifelse(.x == 0, .1, .x))) %>%
    select(NP, income_hh, ethn_hh, age_hh, race_hh, N_HSHLD) %>%
    arrange(NP, income_hh, ethn_hh, age_hh, race_hh) %>%
    tbl_pivot_array(met_name = "N_HSHLD")
  
  all_data_list <- c(all_data_list, 
                     list(tract_data = .x, pums_prsn = pums_prsn, pums_hhld = pums_hhld))
  
})
