su_chart_theme <- function(font_family = "Segoe UI") {
  text_grey <- "#3e3f3a"
  #light_bkg <- "#faf0e6" # aka "linen"
  # su_slate <- su_theme_cols("light_slate")
  dark_slate <- StrategyUnitTheme::su_theme_cols("dark_slate")
  # l_orange <- su_theme_cols("light_orange")
  loran_tr <- "#fcdf8377" # su_theme_cols("light_orange") with transparency
  slate_tr <- "#b2b7b977" # su_theme_cols("light_slate") with transparency
  # coral_tr <- "#ff7f5077" # R coral with transparency
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
    #plot.background = ggplot2::element_rect(fill = light_bkg, colour = NA),
    plot.margin = ggplot2::margin(6, 24, 6, 6),
    #panel.background = ggplot2::element_rect(fill = light_bkg),
    legend.title = ggplot2::element_text(
      hjust = 0,
      margin = ggplot2::margin(b = 8)
    ),
    #legend.background = ggplot2::element_rect(fill = light_bkg),
    # legend.margin = margin(r = 30),
    #legend.key = ggplot2::element_rect(fill = light_bkg),
    legend.key.spacing.y = grid::unit(3, "mm"),
    legend.key.width = grid::unit(12, "mm"),
    legend.text = ggplot2::element_text(
      margin = ggplot2::margin(l = 16),
      hjust = 1
    ),
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
    # panel.grid.major = element_blank(),
    # panel.grid.minor = element_blank(),
    axis.line = ggplot2::element_blank(),
    # axis.line = element_line(
    #   colour = slate_tr,
    #   linewidth = 1.5,
    #   lineend = c("round", "butt")
    # ),
    axis.ticks = ggplot2::element_blank(),
    # axis.ticks = element_line(
    #   colour = slate_tr,
    #   linewidth = 1.5,
    #   lineend = c("round", "butt"),
    # ),
    axis.text.x = ggplot2::element_text(
      size = 18,
      angle = 45,
      vjust = 0.5,
      hjust = 0.5,
      margin = ggplot2::margin(t = 8, b = 16)
    ),
    axis.text.y = ggplot2::element_text(
      size = 18,
      margin = ggplot2::margin(r = 8, l = 16)
    ),
    strip.background = ggplot2::element_rect(fill = dark_slate, colour = NA),
    strip.text = ggplot2::element_text(colour = "grey95", size = 14),
    validate = TRUE
  )
}
