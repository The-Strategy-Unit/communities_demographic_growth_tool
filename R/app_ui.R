app_ui <- function() {
  bslib::page_navbar(
    title = "Communities demographic growth tool",
    lang = "en",
    theme = bslib::bs_theme(bootswatch = "sketchy"),
    bslib::nav_panel(title = "National", nationalUI("national")),
    bslib::nav_panel(title = "ICB level", icbUI("icb")),
    bslib::nav_panel(title = "About", shiny::p("Third page content."))
  )
}
