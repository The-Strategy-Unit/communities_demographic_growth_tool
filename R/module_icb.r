# ICB module

# UI
icb_ui <- function(id) {
  ns <- shiny::NS(id)

  bslib::page_sidebar(
    sidebar = bslib::sidebar(
      shiny::selectInput(
        ns("icb"),
        "ICB",
        choices = icb_list()
      ),
      shiny::selectInput(
        ns("horizon"),
        label = "Horizon year",
        choices = year_list()[-(1:3)], # Only shortest horizon of 3 years
        selected = dplyr::last(year_list())
      ),
      shiny::selectInput(
        ns("measure"),
        label = "Measure",
        choices = c("Contacts", "Patients"),
        selected = "Contacts"
      )
    ),
    bslib::layout_column_wrap(
      width = NULL,
      style = bslib::css(grid_template_columns = "1fr 3fr"),
      height = 300,
      bslib::card(
        shiny::htmlOutput(ns("icb_sentence"))
      ),
      bslib::navset_card_pill(
        placement = "above",
        full_screen = TRUE,
        bslib::nav_panel(
          title = "Totals by age",
          shiny::plotOutput(ns("icb_measure_by_year"))
        ),
        bslib::nav_panel(
          title = "% change by age",
          shiny::plotOutput(ns("percent_change_by_age"))
        ),
        bslib::nav_panel(
          title = "% change by service",
          shiny::plotOutput(ns("percent_change_by_service"))
        ),
        bslib::nav_panel(
          title = "Utilisation",
          shiny::plotOutput(ns("count_per_population"))
        ),
        bslib::nav_panel(
          title = "Data"
        )
      )
    )
  )
}


# Server

icb_server <- function(id) {
  shiny::moduleServer(id, function(input, output, session) {
    get_icb_data <- shiny::reactive({
      get_all_icb_data(measure = input$measure) |>
        dplyr::filter(.data$icb22cdh == input$icb) |>
        dplyr::select(c("icb22cdh", "icb22nm", "data"))
    })

    get_icb_dq_data <- shiny::reactive({
      get_all_icb_data(measure = input$measure) |>
        dplyr::filter(.data$icb22cdh == input$icb) |>
        dplyr::select(!"data")
    })

    output$icb_sentence <- shiny::renderUI({
      get_icb_sentence(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })

    output$icb_measure_by_year <- shiny::renderPlot({
      plot_icb_measure_by_year(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })

    output$percent_change_by_age <- shiny::renderPlot({
      plot_percent_change_by_age(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })

    output$count_per_population <- shiny::renderPlot({
      plot_count_per_population(get_icb_data(), measure = input$measure)
    })

    output$percent_change_by_service <- shiny::renderPlot({
      plot_percent_change_by_service(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })

    output$data_quality_summary_table <- gt::render_gt({
      create_icb_dq_summary_table(get_icb_dq_data(), measure = input$measure)
    })
  })
}
