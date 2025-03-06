create_main_projection_chart <- function(.data, label) {
  conv_lab <- \(x) {
    paste0(format(x, "%Y"), "/", (as.integer(format(x, "%Y")) + 1) %% 100)
  }
  .data |>
    dplyr::mutate(
      across("fin_year", \(x) as.Date(sub("_[0-9]{2}$", "-01-01", x)))
    ) |>
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_contacts", sum),
      .by = c("fin_year", "broad_age_cat")
    ) |>
    ggplot2::ggplot(ggplot2::aes(fin_year, projected_contacts)) +
    ggplot2::geom_line(
      ggplot2::aes(colour = broad_age_cat, group = broad_age_cat),
      linewidth = 1.8
    ) +
    ggplot2::labs(x = NULL, y = NULL) +
    ggplot2::scale_y_continuous(
      labels = scales::label_number(scale = 1e-6, suffix = "mn")
    ) +
    ggplot2::scale_x_date(
      breaks = seq.Date(as.Date("2022-01-01"), by = "5 years", length.out = 5),
      minor_breaks = seq.Date(
        as.Date("2023-01-01"),
        as.Date("2041-01-01"),
        "1 year"
      ),
      labels = conv_lab,
      guide = ggplot2::guide_axis(minor.ticks = TRUE)
    ) +
    StrategyUnitTheme::scale_colour_su(
      name = "Age group",
      guide = ggplot2::guide_legend(nrow = 1)
    ) +
    su_chart_theme()
}

plot_national_contacts_by_year <- function() {
  create_main_projection_chart(get_national_contacts(), "national")
}

plot_icb_contacts_by_year <- function(icb_data) {
  create_main_projection_chart(icb_data[["data"]][[1]], icb_data[["icb22nm"]])
}

plot_percent_change_by_age <- function(icb_data) {
  list(national = get_national_contacts(), icb = icb_data) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) grepl("^20[24]2", x))) |>
    add_age_groups() |>
    dplyr::summarise(
      value = sum(.data[["projected_contacts"]]),
      .by = c("type", "fin_year", "age_group_cat")
    ) |>
    tidyr::pivot_wider(
      id_cols = c("type", "age_group_cat"),
      names_from = "fin_year",
      names_prefix = "yr_"
    ) |>
    dplyr::mutate(
      pct_change = (.data[["yr_2042_43"]] - .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(age_group_cat, pct_change)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::geom_hline(yintercept = 0, linewidth = 0.4) +
    ggplot2::labs(
      x = "Age group",
      y = "% change"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_percent(suffix = ""),
      limits = c(-0.25, NA)
    ) +
    StrategyUnitTheme::scale_fill_su(name = NULL) +
    su_chart_theme() +
    ggplot2::theme(
      legend.position = "inside",
      legend.position.inside = c(0.15, 0.72),
      legend.text = ggplot2::element_text(
        margin = ggplot2::margin(l = 6),
        hjust = 0
      )
    )
}

plot_contacts_per_population <- function(icb_data) {
  list(national = get_national_contacts(), icb = icb_data) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == "2022_23")) |>
    add_age_groups() |>
    dplyr::summarise(
      dplyr::across(c("fin_year_popn", "projected_contacts"), sum),
      .by = c("type", "age_group_cat")
    ) |>
    dplyr::mutate(
      contacts_rate = .data[["projected_contacts"]] / .data[["fin_year_popn"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(age_group_cat, contacts_rate)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::labs(
      x = "Age group",
      y = "Contacts / 1000 people"
    ) +
    ggplot2::scale_y_continuous(labels = scales::label_number(scale = 1e3)) +
    StrategyUnitTheme::scale_fill_su(name = NULL) +
    su_chart_theme() +
    ggplot2::theme(
      legend.position = "inside",
      legend.position.inside = c(0.15, 0.72),
      legend.text = ggplot2::element_text(
        margin = ggplot2::margin(l = 6),
        hjust = 0
      )
    )
}
