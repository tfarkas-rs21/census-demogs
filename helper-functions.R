library(cubelyr)
library(dplyr)

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
# tarr <- array(rpois(12, 10), dim = c(3, 2, 2), # some fake data
#               dimnames = list(nppl = c(1, 2, 3), 
#                               ethn = c("H", "NH"), 
#                               sex = c("M", "F")))
# expand_household(tarr, 1) # run 