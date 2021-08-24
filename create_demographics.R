library(bigrquery)
library(mipfp)

## function is wrapper to cubelyr::as.tbl_cube
## takes a tibble (data) with a single measurement column (met_name)
## pivots to a tbl_cube, extracts array, and names dimensions (if dname = T)
tbl_pivot_array <- function(data, dnames = TRUE, ...) {
  require(cubelyr)
  
  # get a cube
  cube <- data %>%
    as.tbl_cube(x = ., ...)
  
  # extract and name array
  arr <- cube$met[[1]]
  if(dnames) dimnames(arr) <- cube$dims
  return(arr)
  
}

#### expand_household function takes a contingency array of household counts (data)
#### for which one dimension (ppl_var) indicates the number of people per household.
#### requires that ppl_var is a named dimension with simple integer names ("1", "2", ...)
#### ppl_var can be referenced by name or dimension index 
#### returns contingency tables with estimates of people counts with k - 1 dimensions
expand_household <- function(data, ppl_var) {
  apply(data, 
        MARGIN = (1:length(dim(data)))[-ifelse(is.numeric(ppl_var), ppl_var, 
                                               which(names(dimnames(data)) == ppl_var))], 
        FUN = function(x) sum(x * (dimnames(data)[[ppl_var]] %>% as.numeric))) 
}

#test multiplying nppl through marginal 
tarr <- array(rpois(12, 10), dim = c(3, 2, 2), # some fake data
              dimnames = list(nppl = c(1, 2, 3), 
                              ethn = c("H", "NH"), 
                              sex = c("M", "F")))
expand_household(tarr, 1) # run 


# BQ creds and authorization
bq_auth(path =  "~/.ssh/veraset-prototype-b9b60a42ffc6.json")
proj <- "veraset-prototype"

# test 
sql <- "select * from veraset-prototype.geofacts.detailed_estimates_2019_5yr limit 100"
tb <- bq_project_query(proj, sql)

# establish connection to geofacts
geofacts <- dbConnect(
  bigrquery::bigquery(),
  project = "veraset-prototype",
  dataset = "geofacts",
  billing = proj
)

dbListTables(geofacts) # test connection, look at tables

# create objects for focal tables
ests <- tbl(geofacts, "detailed_estimates_2019_5yr") # 2019 detailed census estimates
geos <- tbl(geofacts, "geographies") # all geographies

# pums joint distributions, augmented with household demography
pums <- tbl(geofacts, "pums_augmented_demography") 

# pull all estimates for focal tables only into memory, tracts only
# TODO: don't do this? establish connections independently on demand
est_df_raw <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) %in% 
           c("B19037A"),
         str_sub(geoid, 1, 2) == "14") %>%

  collect()

# chunk up download and save to data frame
est_df_raw <- c(paste0("B01001", LETTERS[1:9]), 
                paste0("B19037", LETTERS[1:9]),  
                "B03002_") %>% 
  map_dfr(~ {
    ests %>%
    filter(str_sub(concept_id, start = 1, end = 7) == .x,
           str_sub(geoid, 1, 2) == "14") %>%
      collect()
  })

est_df_B19037 <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) == "B19037_",
       str_sub(geoid, 1, 2) == "14") %>%
  collect()

est_df_B01001 <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) == "B01001_",
         str_sub(geoid, 1, 2) == "14") %>%
  collect()

save(est_df_raw, file = "~/projects/mothr/mobility/demographics/tract_tables_df.RData")

# load data frame
load("~/projects/mothr/mobility/demographics/tract_tables_df.RData")

# tracts only
tract_ests <- est_df_raw %>%
  # join in others i forgot
  bind_rows(est_df_B19037, est_df_B01001) %>%
  
  # pull state, county, and tract IDs from GEOID column
  mutate(state = str_sub(geoid, 10, 11),
         county = str_sub(geoid, 12, 14),
         tract = str_sub(geoid, 15, 20)) %>%
  
  # join in geography data to get enclosing PUMA 
left_join(geos %>% 
            filter(summary_level == "TRACT") %>%
            select(state, county, puma, tract) %>%
            collect(), 
          by = c("state", "county", "tract")) 

# split data into list by tract, for passing to map()
tract_list <- tract_ests %>%
  mutate(across(value, ~ as.integer(.x))) %>%
  group_by(geoid) %>%
  group_split

save(tract_list, file = "~/projects/mothr/mobility/demographics/tract_tables_list.RData")
load("~/projects/mothr/mobility/demographics/tract_tables_list.RData")

## PUMS data ##
## all the person-level PUMS data
pums_prsn_df <- pums %>%
  arrange(ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh) %>%
  collect()

#### Helpers ####
### define demography helper objects for each marginal distribution 


## race_hh / ethnicity_hh / age_hh / income helpers

income_bins <- c("0-10K", "10-15K", "15-20K", "20-25K", "25-30K", "30-35K", 
                 "35-40K", "40-45K", "45-50K", "50-60K", "60-75K", "75-100K", 
                 "100-125K", "125-150K", "150-200K", "200K+")
income_bins <- c(0, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 100, 125, 150, 200)
income_lookup <- tibble(income_bins, income = c(rep("0-25000", 4), 
                                                rep("25000-50000", 5), 
                                                rep("50000-75000", 2), 
                                                rep("75000-100000", 1), 
                                                rep("100000-150000", 2), 
                                                rep("150000-200000", 1), 
                                                rep("200000-10000000", 1)))
age_bins <- c("0-25", "25-44", "45-64", "65-300")
race_bins <- c("White", "Black", "Native", 
               "Asian", "Islander", "Other", "2 or more")
ethn_bins <- c("NH", "H")


### race by ethnicity: Table B03002
# field names
er....._fields <- paste0("B03002_", 
                         str_pad(as.character(c(3:9, 13:19)), 
                                 3, "left", "0") , 
                         "E")

# create lookup to link field names to demographic categories
er....._df <- tibble(ethn = rep(c("NH", "H"),  each=7), 
                     race = rep(c("White", "Black", "Native", 
                                  "Asian", "Islander", "Other", "2 or more"), 2),
                     field = er....._fields)

### age by sex: Tables B01001A-I
as_fields <- paste0("B01001", LETTERS[1:9], "_") %>%
  map_dfr(~  paste0(.x, 
                    str_pad(as.character(c(3:16, 18:31)), 3, "left", "0"), 
                    "E") %>%
            tibble(field = .)) %>% unlist
  
# get lookup to link field names to demographic categories FOR EACH TABLE
# this includes sex!! need to marginalize later, then combine tables
as_df <- tibble(age = rep(c("00-04", "05-09", "10-14", "15-17", "18-19", 
                             "20-24", "25-29", "30-34", "35-44", "45-54", 
                             "55-64", "65-74", "75-84", "85+"), 18), 
                     sex = rep(rep(c("M", "F"),  each=14), 9)) %>%
  cbind(field = as_fields, .)

### age by sex with fine binning
as_fine_fields <- paste0("B01001_", 
                         str_pad(as.character(c(3:25, 27:49)), 
                                 3, "left", "0") , 
                         "E")

as_fine_df <- tibble(field = as_fine_fields, 
                     age_fine = rep(c("00-04", "05-09", "10-14", "15-17", "18-19", "20", "21", 
                                      "22-24", "25-29", "30-34", "35-39", "40-44", "45-49", 
                                      "50-54", "55-59", "60-61", "62-64", "65-66", "67-69", 
                                      "70-74", "75-79", "80-84", "85+"), 2),
                     age_coarse = rep(c("00-04", "05-09", "10-14", "15-17", "18-19", "20-24", "20-24", 
                                        "20-24", "25-29", "30-34", "35-44", "35-44", "45-54", 
                                        "45-54", "55-64", "55-64", "55-64", "65-74", "65-74", 
                                        "65-74", "75-84", "75-84", "85+"), 2),
                     sex = rep(c("M", "F"), each = 23))

### age_hh by income_hh
...i.a._fields <- paste0("B19037_", 
                    str_pad(as.character(c(3:18, 20:35, 37:52, 54:69)), 
                            3, "left", "0") , 
                    "E")
...i.a._df <- expand_grid(age_hh=age_bins, income=income_bins) %>%
  bind_cols(field = ...i.a._fields)

### age_hh by race_hh by income_hh
...i.ar_fields <- paste0("B19037", LETTERS[1:7], "_") %>%
  map_dfr(~  paste0(.x, 
                str_pad(as.character(c(3:18, 20:35, 37:52, 54:69)), 
                        3, "left", "0") , 
                "E") %>%
        tibble(field = .))

...i.ar_df <- expand_grid(race_bins, age_bins, income_bins) %>%
  bind_cols(field = ...i.ar_fields)

### age_hh by ethn_hh by income_hh

...ie.r_fields <- paste0("B19037", c("", "I"), "_") %>%
  map_dfr(~  paste0(.x, 
                    str_pad(as.character(c(3:18, 20:35, 37:52, 54:69)), 
                            3, "left", "0") , 
                    "E") %>%
            tibble(field = .))

...ie.r_df <- expand_grid(ethn = c("T", "H"), age_hh = age_bins, income = income_bins) %>%
  bind_cols(field = ...ie.r_fields) 

### race_hh by ethn_hh by age_hh by income_hh

...iear_fields <- paste0("B19037", c("A", "H"), "_") %>%
  map_dfr(~  paste0(.x, 
                    str_pad(as.character(c(3:18, 20:35, 37:52, 54:69)), 
                            3, "left", "0") , 
                    "E") %>%
            tibble(field = .))

...iear_df <- expand_grid(ethn = c("T", "H"), race = "White", 
                       age_hh = age_bins, income = income_bins) %>%
  bind_cols(field = ...iear_fields) 

### income_hh by ethn_hh by age_hh by race_hh new approach!
### use all of them in the loop, separate then recombine 
...iear_fields <- paste0("B19037", LETTERS[1:9], "_") %>%
  map_dfr(~  paste0(.x, 
                    str_pad(as.character(c(3:18, 20:35, 37:52, 54:69)), 
                            3, "left", "0") , 
                    "E") %>%
            tibble(field = .)) %>% unlist

...iear_df <- tibble(field = ...iear_fields, 
                     age_hh = rep(rep(age_bins, each = length(income_bins)), 9), 
                     income_hh = rep(income_bins, 9*4))

#### IPF Loop ##########################################################
#### Loop through tracts and calculate joint distributions with IPF ###



# pums_hh <- read_csv("~/projects/mothr/mobility/PUMS/state_household.csv") %>%
#   mutate(across(PUMA, ~ str_pad(as.character(.x), 5, side = "left", pad = "0")), 
#          across(ST, ~ str_pad(as.character(.x), 2, side = "left", pad = "0")))
# save(pums_hh, file = "~/projects/mothr/mobility/PUMS/state_household.RData")

## load the household level PUMS data
load("~/projects/mothr/mobility/PUMS/state_household.RData")


map(tract_list[1], ~{

  # get relevant pums
  thispuma <- .x %>% pull(puma) %>% unique
  thisstate <- .x %>% pull(state) %>% unique
  
  pums_prsn <- pums_prsn_df %>%
    filter(puma == thispuma , 
           state == thisstate) %>%
    arrange(ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh) %>%
    select(-c(totalcount, puma, state)) %>%
    tbl_pivot_array
  
  pums_hhld <- pums_hh %>%
    filter(PUMA == thispuma, 
           ST == thisstate) %>%
    select(NP, income_hh, ethn_hh, age_hh, race_hh, N_HSHLD) %>%
    arrange(income_hh, ethn_hh, age_hh, race_hh, NP) %>%
    tbl_pivot_array(met_name = "N_HSHLD")
  
  # get / create race ethnicity marginal
  er....._marg <- .x %>%
    filter(concept_id %in% er....._fields) %>%
     left_join(er....._df, by = c(concept_id = "field")) %>%
    arrange(ethn, race) %>%
    select(ethn, race, value) %>%
    tbl_pivot_array 
  
  # get / create age_hh by income_hh marginal
  ...i.a._marg <- .x %>%
    filter(concept_id %in% ...i.a._fields) %>%
    left_join(...i.a._df, by = c(concept_id = "field")) %>%
    left_join(income_lookup, by = c("income" = "income_bins")) %>%
    group_by(income.y, age_hh) %>%
    summarize(across(value, sum)) %>%
    rename(income = income.y) %>%
    arrange(income, age_hh) %>%
    tbl_pivot_array 
  
  # get / create age_hh by income marginal
  ...i.a._marg <- .x %>%
    filter(concept_id %in% ...i.a._fields) %>%
    left_join(...i.a._df, by = c(concept_id = "field")) %>%
    left_join(income_lookup, by = c("income" = "income_bins")) %>%
    group_by(income.y, age_hh) %>%
    summarize(across(value, sum)) %>%
    rename(income = income.y) %>%
    arrange(income, age_hh) %>%
    tbl_pivot_array 
  
  #### Race By Ethnicity By Age Marginal
  ### loop through race tables, combine sexes, and infer finer age binning
  
  # get the fine by coarse age seed for IPF
  age_seed <- .x %>%
    filter(concept_id %in% as_fine_fields) %>%
    left_join(as_fine_df, by =c("concept_id"="field")) %>%
    group_by(age_fine, age_coarse) %>%
    summarize(across(value, sum)) %>%
    select(age_fine, age_coarse, value) %>%
    ungroup() %>%
    complete(age_fine, age_coarse, fill = list(value = 0)) %>%
    tbl_pivot_array()
  
  # get the coarse age marginal
  age_tables <- .x %>% 
    filter(concept_id %in% as_fields) %>%
    mutate(table = str_sub(concept_id, 1, 7)) %>%
    left_join(as_df, by = c("concept_id" = "field")) %>%
    group_by(table, age) %>%
    summarize(across(value, sum)) %>%
    ungroup() %>% group_by(table) %>%
    group_split %>% 
    
    # get coarse age tables for A - I
    map(function(.y) {
      age_margin <- .y %>% 
        select(-table) %>% 
        tbl_pivot_array()
      age_dim <- list(2)
      fine_age_cnt <- Ipfp(seed = age_seed, age_dim, list(age_margin))$x.hat %>%
        apply(MARGIN = 1, FUN = sum) 
      tibble(age = c(rep("0-19", 5), 
                     rep("20-39", 6), 
                     rep("40-59", 4), 
                     rep("60-79", 6), 
                     rep("80+", 2)), 
             age_fine = names(fine_age_cnt), 
             value = fine_age_cnt) %>%
        group_by(age) %>%
        summarize(across(value, sum))
    }) %>% set_names(LETTERS[1:9])
  
  ## get age by race marginal
  .ra...._marg <- age_tables[1:7] %>% 
    bind_rows() %>%
    bind_cols(tibble(race = rep(c("White", "Black", "Native", 
                                "Asian", "Islander", "Other", "2 or more"), each = 5)), 
              .) %>% 
    tbl_pivot_array()
  
  ## get ethnicity by age marginal
  e.a...._marg <- age_tables[[9]] %>%
    bind_rows(
  age_tables[1:7] %>% 
    bind_rows() %>%
    group_by(age) %>%
    summarize(across(value, sum))) %>%
    mutate(ethn = rep(c("H", "total"), each = 5)) %>%
    pivot_wider(id_cols = age, names_from = ethn, values_from = value) %>%
    mutate(NH = total - H) %>%
    pivot_longer(cols = c(H, NH), names_to = "ethn") %>%
    select(ethn, age, value) %>%
    tbl_pivot_array()
  
  ## get ethnicity by race by age sparse marginal
  era...._marg 
  age_tables[c(1,8)] %>%
    bind_rows() %>%
    mutate(ethn = rep(c("total", "H"), each = 5), 
           race = "White") %>%
    pivot_wider(id_cols = c(age, race), names_from = ethn, values_from = value) %>%
    mutate(NH = total - H) %>%
    pivot_longer(cols = c(H, NH), names_to = "ethn") %>%
    select(ethn, race, age, value) %>%
    left_join(expand_grid(ethn = c("H", "NH"), 
                          race = c("White", "Black", "Native", 
                                   "Asian", "Islander", "Other", "2 or more"), 
                          age = c("0-19", "20-39", "40-59", "60-79", "80+")), 
              .) %>%
    tbl_pivot_array()
  
  ### Income_HH by Ethn_HH by Age_HH by Race_HH Marginals
  ...iear_tables <-.x %>%
    filter(concept_id %in% ...iear_fields) %>%
    mutate(table = str_sub(concept_id, 1, 7)) %>%
    left_join(...iear_df, by = c("concept_id" = "field")) %>% 
    group_by(table) %>%
    group_split# %>% set_names(LETTERS[1:9])
  
  # get income by age by race
  ...i.ar_marg <- ...iear_tables[1:7] %>%
    bind_rows() %>%
    bind_cols(tibble(race_hh = rep(c("White", "Black", "Native", 
                                  "Asian", "Islander", "Other", "2 or more"), 
                                each = 64))) %>%
    select(income_hh, age_hh, race_hh, value) %>%
    tbl_pivot_array()
    
    
  
  # run household IPF to get 4 dimensional marginal for household variables, 
  # but at the person level (# people with Hispanic head of household, etc.)
  target_hh_dims <- list(c(2, 4))
  target_hh_margs <- list(...i.a._marg)
  hhld_jnt <- Ipfp(seed = pums_hhld, 
       target_hh_dims, target_hh_margs)$x.hat
  ppl_hh_jnt_raw <- expand_household(hhld_jnt, ppl_var = "NP") 
  # correct for almost certain mismatch between tract-estimate for total # of ppl
  # and values derived from IPF using PUMA seed
  ppl_hh_jnt_cor <- ppl_hh_jnt_raw * (sum(er....._marg) / sum(ppl_hh_jnt_raw)) 
  
  # run person IPF 
  target_prsn_dims <- list(c(1, 2), 4:7)
  target_prsn_margs <- list(er....._marg, ppl_hh_jnt_cor)
  Ipfp(seed = pums_prsn, 
       target_prsn_dims, target_prsn_margs)$x.hat %>%
    apply(X = ., MARGIN = c(1, 2, 3, 4), FUN = sum)
  
})
  
apply(pums_jnt, MARGIN = c(4, 6), FUN = sum)

#### Calculate person counts from household counts ####

hhd <- read_csv("~/projects/mothr/mobility/PUMS/nm_household_data.csv", 
                col_types = c(NP = "c")) 

puma100 <- hhd %>% filter(PUMA == "00100") %>%
  mutate(across(NP, ~ str_pad(.x, 2, "left", "0"))) %>% 
  arrange(PUMA, age_hh, race_hh, ethn_hh, income, NP) %>%
  select(NP:age_hh, N_HSHLD) #%>% 
 
  #distinct() %>%
  #arrange(age_hh, race_hh, ethn_hh, income, NP) %>% View
  #arrange(NP, income, ethn_hh, race_hh, age_hh) %>% View
  tbl_pivot_array(data = ., met_name = "N_HSHLD")

...i.a._marg <- .x %>%
  filter(concept_id %in% ...i.a._fields) %>%
  left_join(...i.a._df, by = c(concept_id = "field")) %>%
  left_join(income_lookup, by = c("income" = "income_bins")) %>%
  group_by(income.y, age_hh) %>%
  summarize(across(value, sum)) %>%
  rename(income = income.y) %>%
  arrange(age_hh, income) %>%
  tbl_pivot_array 

target_dims <- list(c(2, 5))
target_margs <- list(...i.a._marg)
hhout <- Ipfp(seed = puma100, 
     target_dims, target_margs)$x.hat

pplout <- expand_household(hhout, ppl_var = "NP")


### compare Tim and Sravani PUMS
puma100_ST <- read_csv("~/projects/mothr/mobility/PUMS/pums_100.csv")

### test overlapping / non-partitioned distribution estimation
## OK use this to infer finer age bins from coarser. 

yr5bins <- matrix(c(1, 5, 0, 0, 0, 0, 5, 7), nrow = 4)
target_dims <- list(2)
target_margs <- list(c(10, 20))
Ipfp(seed = yr5bins, 
     target_dims, target_margs)$x.hat

