library(dplyr)
library(purrr)
library(lubridate)

flatten_mb <- function(result) {
  ## unlisting track data columns of class list
  if(any(sapply(mt_track_data(result), is_bare_list))){
    ## reduce all columns were entry is the same to one (so no list anymore)
    result <- result |> mutate_track_data(across(
      where( ~is_bare_list(.x) && all(purrr::map_lgl(.x, function(y) 1==length(unique(y)) ))),
      ~do.call(vctrs::vec_c,purrr::map(.x, head,1))))
    if(any(sapply(mt_track_data(result), is_bare_list))){
      ## transform those that are still a list into a character string
      result <- result |> mutate_track_data(across(
        where( ~is_bare_list(.x) && any(purrr::map_lgl(.x, function(y) 1!=length(unique(y)) ))),
        ~unlist(purrr::map(.x, paste, collapse=","))))
    }
  }
  return(result)
}
