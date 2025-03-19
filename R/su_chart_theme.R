su_chart_theme <- function(font_family = "Segoe UI") {
  text_grey <- "#3e3f3a"
  light_bkg <- "grey95"
  dark_slate <- StrategyUnitTheme::su_theme_cols("dark_slate")
  loran_tr <- "#fcdf8377" # su_theme_cols("light_orange") with transparency
  slate_tr <- "#b2b7b977" # su_theme_cols("light_slate") with transparency
  ggplot2::theme(
    text = ggplot2::element_text(
      family = font_family,
      size = 20,
      colour = text_grey
    ),
    title = ggplot2::element_text(hjust = 0.05),
    plot.title = ggplot2::element_text(
      size = 28,
      margin = ggplot2::margin(20, 8, 12, 18)
    ),
    plot.subtitle = ggplot2::element_text(
      margin = ggplot2::margin(2, 0, 12, 20)
    ),
    plot.caption = ggplot2::element_text(
      hjust = 0.96,
      size = 16,
      margin = ggplot2::margin(12, 6, 6, 0)
    ),
    plot.title.position = "plot",
    plot.caption.position = "plot",
    plot.background = ggplot2::element_rect(fill = light_bkg, colour = NA),
    plot.margin = ggplot2::margin(6, 24, 6, 6),
    panel.background = ggplot2::element_rect(fill = light_bkg),
    legend.position = "bottom",
    legend.title.position = "top",
    legend.title = ggplot2::element_text(
      hjust = 0,
      margin = ggplot2::margin(b = 8)
    ),
    legend.text.position = "bottom",
    legend.background = ggplot2::element_rect(fill = light_bkg),
    legend.key = ggplot2::element_rect(fill = light_bkg),
    legend.key.spacing.y = grid::unit(3, "mm"),
    legend.key.width = grid::unit(12, "mm"),
    legend.direction = "horizontal",
    panel.grid.major = ggplot2::element_line(
      colour = slate_tr,
      linewidth = 0.2,
      lineend = "butt"
    ),
    panel.grid.minor = ggplot2::element_line(
      colour = loran_tr,
      linewidth = 0.1,
      lineend = "butt"
    ),
    axis.line = ggplot2::element_blank(),
    axis.ticks.y = ggplot2::element_blank(),
    axis.ticks.x = ggplot2::element_line(
      colour = text_grey,
      linewidth = 1.2,
      lineend = "butt"
    ),
    axis.ticks.length = grid::unit(5, "mm"),
    axis.minor.ticks.x.bottom = ggplot2::element_line(colour = slate_tr),
    axis.minor.ticks.length = grid::unit(3, "mm"),
    axis.text.x = ggplot2::element_text(
      size = 18,
      margin = ggplot2::margin(t = 12, b = 8)
    ),
    axis.text.y = ggplot2::element_text(
      size = 18,
      margin = ggplot2::margin(r = 12, l = 8)
    ),
    strip.background = ggplot2::element_rect(fill = dark_slate, colour = NA),
    strip.text = ggplot2::element_text(colour = "grey95", size = 14),
    validate = TRUE
  )
}
