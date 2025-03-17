# National module

# UI
national_ui <- function(id) {
  ns <- shiny::NS(id)

  bslib::page_fillable(
    bslib::layout_column_wrap(
      width = 0.5,
      height = 300,
      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Projected contacts (national) by broad age group, 2022/23 - 2042/43"
        ),
        shiny::plotOutput(ns("national_contacts_by_year")),
      )
    )
  )
}


# Server

national_server <- function(id) {
  shiny::moduleServer(id, function(input, output, session) {
    output$national_contacts_by_year <- shiny::renderPlot({
      plot_national_contacts_by_year()
    })
  })
}
