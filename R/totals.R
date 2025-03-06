get_total_fy_contacts <- function(dat, fin_year) {
  dat[["data"]][[1]] |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == fin_year)) |>
    dplyr::pull("projected_contacts") |>
    sum()
}
get_national_total_fy_contacts <- function(fin_year = "2022_23") {
  get_total_fy_contacts(get_national_data(), fin_year)
}

get_icb_total_fy_contacts <- function(icb_data, fin_year = "2022_23") {
  get_total_fy_contacts(icb_data, fin_year)
}

get_pct_change_total_contacts <- \(x, y) round((y - x) * 100 / x, 1)
