app_ui <- function() {
  bslib::page_navbar(
    title = "Community services demographic growth",
    lang = "en",
    theme = bslib::bs_theme(brand = TRUE),
    bslib::nav_panel(title = "National", national_ui("national")),
    bslib::nav_panel(title = "ICB", icb_ui("icb")),
    bslib::nav_panel(title = "About", shiny::p("Third page content."))
  )
}
