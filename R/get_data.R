get_contacts_data <- function() {
  CSDSDemographicGrowthApp::contacts_fy_projected_icb |>
    dplyr::filter(dplyr::if_any("consistent")) |>
    dplyr::filter(attendance_status %in% c(5, 6)) |>
    dplyr::select(-consistent) |>
    dplyr::filter(!is.na(icb22cdh)) |>
    tidyr::nest(data = -c(icb22cdh, icb22nm))
}

icb_list <- function() {
  get_contacts_data() |>
    dplyr::select(c(icb22nm, icb22cdh)) |>
    tibble::deframe()
}

get_national_contacts <- function() {
  get_contacts_data() |>
    dplyr::pull("data") |>
    dplyr::bind_rows() |>
    dplyr::summarise(
      dplyr::across(c("fin_year_popn", "projected_contacts"), sum),
      .by = c("fin_year", "age_int")
    )
}
