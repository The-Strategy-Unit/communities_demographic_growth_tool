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
  fmt <- \(x) round(x, -5) / 1e6 # nolint
  bas <- get_national_baseline_count(measure)
  hrz <- get_national_horizon_count(measure)
  percent_change <- round((hrz - bas) * 100 / bas, 1) # nolint

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

get_icb_age_group_change <- function(dat, measure, horizon) {
  dat |>
    pluck_data() |>
    prepare_plot_data(measure) |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
    add_age_groups() |>
    dplyr::summarise(
      value = sum(.data$projected_count),
      .by = c("fin_year", "age_group_cat")
    ) |>
    tidyr::pivot_wider(
      id_cols = "age_group_cat",
      names_from = "fin_year",
      names_prefix = "yr_"
    ) |>
    dplyr::mutate(
      pct_change = (.data[[glue::glue("yr_{horizon}")]] -
        .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    ) |>
    dplyr::mutate(dplyr::across("pct_change", \(x) x * 100)) |>
    dplyr::slice_max(.data$pct_change) |>
    tibble::deframe()
}


get_icb_service_change <- function(dat, measure, horizon) {
  dat |>
    pluck_data() |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
    tidyr::unnest("data") |>
    dplyr::filter(!is.na(.data$service)) |>
    dplyr::mutate(
      dplyr::across("service", \(x) sub(" Service", "", x)),
      dplyr::across("service", \(x) sub("Adult's", "Adults", x)),
      dplyr::across("service", \(x) sub("Children's", "Children", x))
    ) |>
    dplyr::summarise(
      value = sum(.data$projected_count),
      .by = c("fin_year", "service")
    ) |>
    tidyr::pivot_wider(
      id_cols = "service",
      names_from = "fin_year",
      names_prefix = "yr_"
    ) |>
    dplyr::mutate(
      pct_change = (.data[[glue::glue("yr_{horizon}")]] -
        .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    ) |>
    dplyr::mutate(dplyr::across("pct_change", \(x) x * 100)) |>
    dplyr::slice_max(.data$pct_change) |>
    tibble::deframe()
}

get_dq_exclusion_rate <- function(dat, measure) {
  total <- dat[[glue::glue("icb_{tolower(measure)}_all_unfiltd")]]
  excl <- dat[[glue::glue("icb_{tolower(measure)}_total_excld")]]
  round(excl * 100 / total, 1)
}


get_icb_sentence <- function(dat, measure, horizon) {
  fmt <- \(x) format(round(x, -3), big.mark = ",")
  bas <- get_total_fy_count(dat, "2022_23")
  hrz <- get_total_fy_count(dat, horizon)
  overall_pct_change <- round((hrz - bas) * 100 / bas, 1)
  age_grp_pct_change <- get_icb_age_group_change(dat, measure, horizon)
  service_pct_change <- get_icb_service_change(dat, measure, horizon)
  dq_exclusion_rate <- get_dq_exclusion_rate(dat, measure)
  horizon <- stringr::str_replace(horizon, "_", "/")

  glue::glue(
    "<p> For <strong>{dat$icb22nm}</strong> the total <strong>{tolower(measure)}
    </strong> estimated by <strong>{horizon}</strong> due to demand from
    population growth is <strong>{fmt(hrz)}</strong>. This is an increase of
    <strong>{overall_pct_change}%</strong> above the 2022/23 baseline of
    <strong>{fmt(bas)}</strong>. However, this increase is an all-age figure
    and the percentage change can differ noticeably between age groups.</p>

    <p>We can look at the percentage change by age for <strong>{dat$icb22nm}
    </strong> in more detail and compare it to the change across England as a
    whole. The age group with the largest percentage change in <strong>
    {tolower(measure)}</strong> by <strong>{horizon}</strong> is <strong>
    {names(age_grp_pct_change)}</strong> with a difference of <strong>
    {round(age_grp_pct_change, 1)}%</strong>.</p>

    <p>We can also consider the breakdown by service. For <strong>{dat$icb22nm}
    </strong>, the service with the largest percentage change in <strong>
    {tolower(measure)}</strong> by <strong>{horizon}</strong> is <strong>
    {names(service_pct_change)}</strong> with a difference of <strong>
    {round(service_pct_change, 1)}%</strong>.</p>

    <p>The population usage rate plot gives an idea of service usage across age
    groups. This is calculated by dividing the number of <strong>
    {tolower(measure)}</strong> by the population in the baseline year 2022/23.
    However, as we know the data in the baseline year is an under-estimate due
    to data quality issues, this graph will also underestimate the true
    utilisation of community services - hence our decision to not show actual numbers.</p>

    <p>The patient usage rate plot shows the number of contacts per 1,000
    patients in each age group, based on 2022/23 data only. The data for
    <strong>{dat$icb22nm}</strong> can be compared to rates for England as a
    whole. While the pattern may vary from ICB to ICB, in general it is
    patients in the older age groups who have more contacts with community
    services.</p>

    <p>Finally, data quality statistics are reported to demonstrate issues in
    data reporting. <strong>{dq_exclusion_rate}%</strong> of all initial
    {tolower(measure)} in the <strong>{dat$icb22nm}</strong>'s data were
    excluded due to data quality issues.</p>"
  ) |>
    htmltools::HTML()
}
