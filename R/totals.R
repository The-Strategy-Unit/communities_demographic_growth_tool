get_total_fy_contacts <- function(dat, fin_year) {
  dat[["data"]][[1]] |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == .env$fin_year)) |>
    dplyr::pull("projected_contacts") |>
    sum()
}
get_national_total_fy_contacts <- function(fin_year = "2022_23") {
  get_total_fy_contacts(get_national_data(), fin_year)
}

get_icb_total_fy_contacts <- function(icb_data, fin_year = "2022_23") {
  get_total_fy_contacts(icb_data, fin_year)
}

get_icb_pct_change_total_fy_contacts <- function(
  icb_data,
  baseline_year = "2022_23",
  horizon_year = "2042_43"
) {
  b <- get_icb_total_fy_contacts(icb_data, baseline_year)
  h <- get_icb_total_fy_contacts(icb_data, horizon_year)
  round((h - b) * 100 / b, 1)
}
get_national_pct_change_total_fy_contacts <- function(
  baseline_year = "2022_23",
  horizon_year = "2042_43"
) {
  b <- get_national_total_fy_contacts(baseline_year)
  h <- get_national_total_fy_contacts(horizon_year)
  round((h - b) * 100 / b, 1)
}

get_icb_sentence <- function(dat, horizon) {
  total_baseline <-
    get_icb_total_fy_contacts(dat, "2022_23") |>
    round(-3) |>
    format(big.mark = ",")

  total_horizon <-
    get_icb_total_fy_contacts(dat, horizon) |>
    round(-3) |>
    format(big.mark = ",")

  percent_change <-
    get_icb_pct_change_total_fy_contacts(
      dat,
      baseline_year = "2022_23",
      horizon_year = horizon
    )

  glue::glue(
    "The total number of contacts for {dat$icb22nm} is {total_baseline}. <br><br>
    By the year {stringr::str_replace(horizon, '_', '/')} this is predicted to rise to {total_horizon},
     an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}
