plot_national_contacts_by_year <- function() {
  get_national_contacts() |>
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
      labels = scales::label_number(scale = 1e-6, suffix = "mn"),
      limits = c(0, 6e7)
    ) +
    ggplot2::scale_x_discrete(
      breaks = \(x) x[seq(1, length(x), 5)],
      labels = \(x) sub("_", "/", x)
    ) +
    StrategyUnitTheme::scale_colour_su(name = "Age group") +
    ggplot2::theme(
      legend.position = "inside",
      legend.position.inside = c(0.9, 0.6)
    ) +
    su_chart_theme()
}

plot_icb_contacts_by_year <- function(icb_data) {
  icb_data |>
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
      labels = scales::label_number(scale = 1e-3, suffix = "k"),
      limits = c(0, NA)
    ) +
    ggplot2::scale_x_discrete(
      breaks = \(x) x[seq(1, length(x), 5)],
      labels = \(x) sub("_", "/", x)
    ) +
    StrategyUnitTheme::scale_colour_su(name = "Age group") +
    su_chart_theme()
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
