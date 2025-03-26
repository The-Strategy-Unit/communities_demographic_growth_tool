get_total_fy_count <- function(dat, fy) {
  count_col <- paste0("projected_", type)
  dat |>
    pluck_data() |>
    dplyr::filter(.data[["fin_year"]] == {{ fy }}) |>
    dplyr::reframe(across("data", dplyr::bind_rows)) |>
    purrr::pluck("data", count_col) |>
    sum()
}
get_icb_total_fy_count <- function(icb_data, fy = "2022_23") {
  get_total_fy_count(icb_data, fy)
}
get_national_total_fy_count <- function(fy = "2022_23") {
  get_total_fy_count(get_all_national_data(), fy)
}

get_icb_pct_change_total_fy <- function(
  icb_data,
  baseline_year = "2022_23",
  horizon_year = "2042_43"
) {
  b <- get_icb_total_fy_count(icb_data, baseline_year)
  h <- get_icb_total_fy_count(icb_data, horizon_year)
  round((h - b) * 100 / b, 1)
}
get_national_pct_change_fy <- function(
  baseline_year = "2022_23",
  horizon_year = "2042_43"
) {
  nat_data <- get_all_national_data()
  b <- get_national_total_fy_count(nat_data, baseline_year)
  h <- get_national_total_fy_count(nat_data, horizon_year)
  round((h - b) * 100 / b, 1)
}

get_icb_sentence <- function(dat, horizon) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_icb_total_fy_count(dat, "2022_23")
  hrz <- get_icb_total_fy_count(dat, horizon)
  percent_change <- round((hrz - bas) * 100 / bas, 1)
  horizon <- stringr::str_replace(horizon, '_', '/')

  glue::glue(
    "The total number of contacts for {dat$icb22nm} is {fmt(bas)}.<br /><br />
      By the year {horizon} this is predicted to rise to {fmt(hrz)},
      an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}
