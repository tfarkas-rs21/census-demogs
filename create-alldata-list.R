library(dplyr)
library(purrr)
library(cubleyr)

# load required data
load("~/projects/mothr/mobility/census-demogs/data/pums_hhld.RData")
load("~/projects/mothr/mobility/census-demogs/data/pums_prsn.RData")
load("~/projects/mothr/mobility/census-demogs/data/tract_tables_list.RData")
source("~/projects/mothr/mobility/census-demogs/helper-functions.R")

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

### an alternate approach for parallelization

# get tract mapping to pums
tract_puma_df <- tract_list %>%
  map_df( ~ {
    .x %>%
      distinct(state, county, tract, puma) %>%
      mutate(geoid = paste0(state, county, tract),
               puma_id = ifelse(is.na(puma), "NA", paste0(state, puma)))
  })

save(tract_puma_df, file = "~/projects/mothr/mobility/census-demogs/data/tract_puma_mapping.RData")

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

###### name tract list
tract_list <- tract_list %>%
  as.list %>%
  set_names(tract_puma_df %>% pull(geoid))

save(tract_list, file = "~/projects/mothr/mobility/census-demogs/data/tract_tables_list.RData")

###### test permute


ts <- tract_list[1:3] %>% as.list
ph <- pums_hh_list_expanded[1:3]
