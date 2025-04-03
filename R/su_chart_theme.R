su_chart_theme <- function(font_family = "Segoe UI") {
  slate <- "#686f73"
  dark_slate <- "#343739"
  light_slate <- "#b2b7b9"
  ggplot2::theme(
    text = ggplot2::element_text(
      family = font_family,
      size = 20,
      colour = dark_slate
    ),
    title = ggplot2::element_text(hjust = 0.05),
    panel.background = ggplot2::element_rect(fill = "#f5f4f2"),
    plot.background = ggplot2::element_rect(fill = "#f5f4f2"),
    legend.background = ggplot2::element_rect(fill = "#f5f4f2"),
    legend.position = "bottom",
    legend.title.position = "left",
    legend.text.position = "bottom",
    legend.key.spacing.x = grid::unit(5, "mm"),
    legend.key.spacing.y = grid::unit(3, "mm"),
    legend.key.width = grid::unit(20, "mm"),
    legend.direction = "horizontal",
    panel.grid.major.y = ggplot2::element_line(
      colour = light_slate,
      linewidth = 0.4
    ),
    panel.grid.major.x = ggplot2::element_blank(),
    panel.grid.minor.y = ggplot2::element_line(
      colour = light_slate,
      linewidth = 0.2
    ),
    panel.grid.minor.x = ggplot2::element_blank(),
    axis.line = ggplot2::element_blank(),
    axis.ticks.y = ggplot2::element_blank(),
    axis.ticks.x = ggplot2::element_line(colour = slate),
    axis.ticks.length = grid::unit(3, "mm"),
    axis.minor.ticks.x.bottom = ggplot2::element_blank(),
    axis.text.x = ggplot2::element_text(
      colour = dark_slate,
      margin = ggplot2::margin(t = 12, b = 8)
    ),
    axis.text.y = ggplot2::element_text(
      margin = ggplot2::margin(r = 12, l = 8)
    ),
    strip.background = ggplot2::element_rect(fill = dark_slate, colour = NA),
    strip.text = ggplot2::element_text(colour = "grey95", size = 14),
    validate = TRUE
  )
}
