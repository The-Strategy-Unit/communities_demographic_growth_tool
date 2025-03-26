# National module

# UI
national_ui <- function(id) {
  ns <- shiny::NS(id)
  options(scipen = 999)
  bslib::page_fillable(
    bslib::layout_columns(
      fill = FALSE,
      bslib::value_box(
        title = "Total contacts",
        value = format(
          round(get_national_total_fy_contacts(), -5),
          big.mark = ","
        ),
        showcase = bsicons::bs_icon("bandaid-fill"),
        shiny::p("in 2022/23")
      ),
      bslib::value_box(
        title = "Total contacts",
        value = format(
          round(get_national_total_fy_contacts(fin_year = "2042_43"), -5),
          big.mark = ","
        ),
        showcase = bsicons::bs_icon("graph-up"),
        shiny::p("in 2042/43")
      ),
      bslib::value_box(
        title = "Forecast increase",
        value = glue::glue("{get_national_pct_change_total_fy_contacts()}%"),
        showcase = bsicons::bs_icon("arrow-up-right"),
        shiny::p("by 2042/43")
      ),
      bslib::value_box(
        title = "Total patients",
        value = 1400,
        showcase = bsicons::bs_icon("people-fill"),
        shiny::p("* placeholder figure")
      )
    ),
    bslib::layout_columns(
      bslib::navset_card_pill(
        title = "National projections by broad age group",
        placement = "above",
        full_screen = TRUE,
        bslib::nav_panel(
          title = "Contacts",
          shiny::htmlOutput(ns("national_sentence")),
          shiny::plotOutput(ns("national_contacts_by_year"))
        ),
        bslib::nav_panel(title = "Patients", shiny::p("Patients plot")),
        bslib::nav_panel(
          title = "Data",
          shiny::downloadButton("downloadData", "Download")
        )
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

    output$national_sentence <- shiny::renderUI({
      get_national_sentence()
    })
  })
}
