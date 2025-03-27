get_total_fy_count <- function(dat, fy) {
  dat |>
    pluck_data() |>
    dplyr::filter(.data[["fin_year"]] == {{ fy }}) |>
    dplyr::reframe(dplyr::across("data", dplyr::bind_rows)) |>
    purrr::pluck("data", "projected_count") |>
    sum()
}

get_national_baseline_count <- function(measure) {
  get_total_fy_count(get_all_national_data(measure), "2022_23")
}
get_national_horizon_count <- function(measure) {
  get_total_fy_count(get_all_national_data(measure), "2042_43")
}
get_national_pct_change <- function(measure) {
  bas <- get_national_baseline_count(measure)
  hrz <- get_national_horizon_count(measure)
  round((hrz - bas) * 100 / bas, 1)
}

get_national_sentence <- function(measure = c("Contacts", "Patients")) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_national_baseline_count(measure)
  hrz <- get_national_horizon_count(measure)
  percent_change <- round((hrz - bas) * 100 / bas, 1)

  glue::glue(
    "The total number of {tolower(measure)} for England is {fmt(bas)}.
      <br /><br />
      By the year 2042/43 this is predicted to rise to {fmt(hrz)},
      an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}

get_icb_sentence <- function(dat, horizon) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_total_fy_count(dat, "2022_23")
  hrz <- get_total_fy_count(dat, horizon)
  percent_change <- round((hrz - bas) * 100 / bas, 1)
  horizon <- stringr::str_replace(horizon, "_", "/")

  glue::glue(
    "The total number of contacts for {dat$icb22nm} is {fmt(bas)}.<br /><br />
      By the year {horizon} this is predicted to rise to {fmt(hrz)},
      an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}
