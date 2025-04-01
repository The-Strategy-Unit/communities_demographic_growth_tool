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

get_national_sentence_data <- function(measure = c("Contacts", "Patients")) {
  dat <- get_all_national_data(measure) |>
    pluck_data() |>
    prepare_plot_data(measure)

  all_baseline <- dat |>
    dplyr::filter(.data$fin_year == "2022_23") |>
    dplyr::summarise(dplyr::across("projected_count", sum)) |>
    dplyr::pull("projected_count")
  baseline_age_group_counts <- dat |>
    dplyr::filter(.data$fin_year == "2022_23") |>
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = "broad_age_cat"
    ) |>
    tibble::deframe()
  baseline_age_group_props <- baseline_age_group_counts / all_baseline

  all_midpoint <- dat |>
    dplyr::filter(.data$fin_year == "2032_33") |>
    dplyr::summarise(dplyr::across("projected_count", sum)) |>
    dplyr::pull("projected_count")
  midpoint_age_group_counts <- dat |>
    dplyr::filter(.data$fin_year == "2032_33") |>
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = "broad_age_cat"
    ) |>
    tibble::deframe()
  midpoint_age_group_props <- midpoint_age_group_counts / all_midpoint

  all_horizon <- dat |>
    dplyr::filter(.data$fin_year == "2042_43") |>
    dplyr::summarise(dplyr::across("projected_count", sum)) |>
    dplyr::pull("projected_count")
  horizon_age_group_counts <- dat |>
    dplyr::filter(.data$fin_year == "2042_43") |>
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = "broad_age_cat"
    ) |>
    tibble::deframe()
  horizon_age_group_props <- horizon_age_group_counts / all_horizon

  list(
    baseline_counts = baseline_age_group_counts,
    midpoint_counts = midpoint_age_group_counts,
    horizon_counts = horizon_age_group_counts,
    baseline_props = baseline_age_group_props,
    midpoint_props = midpoint_age_group_props,
    horizon_props = horizon_age_group_props
  )
}


get_national_sentence <- function(measure = c("Contacts", "Patients")) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_national_baseline_count(measure)
  hrz <- get_national_horizon_count(measure)
  percent_change <- round((hrz - bas) * 100 / bas, 1)

  glue::glue_data(
    get_national_sentence_data(measure),
    "<p>The total number of {tolower(measure)} for England in the CSDS for ",
    "2022/23 was {fmt(bas)}m.<br />",
    "By the year 2042/43 this is projected to rise to {fmt(hrz)}m, an ",
    "increase of {percent_change}%.</p>",
    "<p>{measure} within the two older age groups, ",
    "<span style='color: #5881c1;'><strong>65-84</strong></span> and ",
    "<span style='color: #ec6555;'><strong>85+</strong></span>, together ",
    "accounted for {round(sum(baseline_counts[4:5]), -5) / 1e6}m community ",
    "services {tolower(measure)} ({round(sum(baseline_props[4:5]) * 100, 1)}% ",
    "of all {tolower(measure)}) in 2022/23, projected to rise to ",
    "{round(sum(midpoint_counts[4:5]), -5) / 1e6}m ",
    "({round(sum(midpoint_props[4:5]) * 100, 1)}% of total) in 2032/33 and ",
    "further to {round(sum(horizon_counts[4:5]), -5) / 1e6}m ",
    "({round(sum(horizon_props[4:5]) * 100, 1)}% of total) by 2042/43.</p>"
  ) |>
    htmltools::HTML()
}

get_icb_sentence <- function(dat, measure, horizon) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_total_fy_count(dat, "2022_23")
  hrz <- get_total_fy_count(dat, horizon)
  percent_change <- round((hrz - bas) * 100 / bas, 1)
  horizon <- stringr::str_replace(horizon, "_", "/")

  glue::glue(
    "The total number of {tolower(measure)} for {dat$icb22nm} in the CSDS for
      2022/23 was {fmt(bas)}.<br /><br />
      By the year {horizon} this is projected to rise to {fmt(hrz)},
      an increase of {percent_change}%."
  ) |>
    htmltools::HTML()
}
