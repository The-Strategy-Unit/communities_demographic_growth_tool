get_all_icbs_data <- function() {
  CSDSDemographicGrowthApp::icb_projected_contacts_fy |>
    dplyr::mutate(
      dplyr::across("icb22nm", \(x) sub("^(NHS )(.*)( Integ.*)$", "\\2 ICB", x))
    )
}

icb_list <- function() {
  get_all_icbs_data() |>
    dplyr::select(c("icb22nm", "icb22cdh")) |>
    tibble::deframe()
}

year_list <- function() {
  years <-
    get_national_contacts() |>
    dplyr::pull("fin_year") |>
    unique()
  names(years) <- stringr::str_replace(years, "_", "/")
  years
}

get_national_data <- \() CSDSDemographicGrowthApp::nat_projected_contacts_fy
get_national_contacts <- function() {
  CSDSDemographicGrowthApp::nat_projected_contacts_fy[["data"]][[1]]
}
