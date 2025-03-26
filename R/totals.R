get_total_fy_count <- function(dat, fy) {
  dat |>
    pluck_data() |>
    dplyr::filter(.data[["fin_year"]] == {{ fy }}) |>
    dplyr::reframe(across("data", dplyr::bind_rows)) |>
    purrr::pluck("data", "projected_count") |>
    sum()
}

get_national_sentence <- function() {
  dat <- get_all_national_data("Contacts")
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_total_fy_count(dat, "2022_23")
  hrz <- get_total_fy_count(dat, "2042_43")
  percent_change <- round((hrz - bas) * 100 / bas, 1)

  glue::glue(
    "The total number of contacts for England is {fmt(bas)}.<br /><br />
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
  horizon <- stringr::str_replace(horizon, '_', '/')

  glue::glue(
    "The total number of contacts for {dat$icb22nm} is {fmt(bas)}.<br /><br />
      By the year {horizon} this is predicted to rise to {fmt(hrz)},
      an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}
