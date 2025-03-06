get_all_icbs_data <- function() {
  icb_projected_contacts_fy |>
    dplyr::mutate(
      dplyr::across("icb22nm", \(x) sub("^(NHS )(.*)( Integ.*)$", "\\2 ICB", x))
    )
}

icb_list <- function() {
  get_all_icbs_data() |>
    dplyr::select(c("icb22nm", "icb22cdh")) |>
    tibble::deframe()
}

get_national_data <- \() nat_projected_contacts_fy
get_national_contacts <- \() nat_projected_contacts_fy[["data"]][[1]]
