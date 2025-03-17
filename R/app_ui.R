app_ui <- function() {
  bslib::page_navbar(
    title = "Communities demographic growth tool",
    lang = "en",
    theme = bslib::bs_theme(bootswatch = "sketchy"),
    bslib::nav_panel(title = "National", national_ui("national")),
    bslib::nav_panel(title = "ICB level", icb_ui("icb")),
    bslib::nav_panel(title = "About", shiny::p("Third page content."))
  )
}
