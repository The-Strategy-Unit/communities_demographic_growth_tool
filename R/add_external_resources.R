#' Add external Resources to the Application
#'
#' This function is internally used to add external
#' resources inside the Shiny application.
add_external_resources <- function() {
  shiny::addResourcePath(
    "www",
    app_sys("www")
  )
  shiny::singleton(
    shiny::tags$head(
      shiny::tags$script(type = "text/javascript", src = "www/logo.js"),
    )
  )
  shinyjs::useShinyjs()
}

#' @noRd
app_sys <- function(...) {
  system.file(..., package = "CSDSDemographicGrowthApp")
}
