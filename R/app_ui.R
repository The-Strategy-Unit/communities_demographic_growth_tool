app_ui <- function() {
  bslib::page_navbar(
    title = "Community services demographic growth",
    lang = "en",
    theme = bslib::bs_theme(brand = TRUE),
    header = add_external_resources(),
    bslib::nav_panel(title = "National", national_ui("national")),
    bslib::nav_panel(title = "ICB", icb_ui("icb")),
    bslib::nav_panel(
      title = "About",
      bslib::layout_columns(
        col_widths = c(-3, 6, -3),
        shiny::includeMarkdown(app_sys("www/methodology.md"))
      )
    )
  )
}
