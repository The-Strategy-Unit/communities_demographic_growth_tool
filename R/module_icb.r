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
      height = 300,
      bslib::navset_card_pill(
        placement = "above",
        full_screen = TRUE,
        bslib::nav_panel(
          title = "Totals by age",
          shiny::htmlOutput(ns("total_paragraph")),
          shiny::plotOutput(ns("icb_measure_by_year"))
        ),
        bslib::nav_panel(
          title = "% change by age",
          shiny::htmlOutput(ns("age_paragraph")),
          shiny::plotOutput(ns("percent_change_by_age"))
        ),
        bslib::nav_panel(
          title = "% change by service",
          shiny::htmlOutput(ns("service_paragraph")),
          shiny::plotOutput(ns("percent_change_by_service"))
        ),
        bslib::nav_panel(
          title = "Population usage rate",
          shiny::htmlOutput(ns("population_usage_paragraph")),
          shiny::plotOutput(ns("count_per_population"))
        ),
        bslib::nav_panel(
          title = "Patient usage rate",
          shiny::htmlOutput(ns("patient_usage_paragraph")),
          shiny::plotOutput(ns("contacts_per_patient"))
        ),
        bslib::nav_panel(
          title = "Data quality",
          shiny::htmlOutput(ns("data_quality_paragraph")),
          gt::gt_output(ns("data_quality_summary_table"))
        ),
        footer = bslib::card_body(
          fill = FALSE,
          shiny::includeText(app_sys("www/warning-note.txt"))
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
        dplyr::collect()
    })

    # Overall
    output$total_paragraph <- shiny::renderUI({
      get_icb_paragraph_total(
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

    # Age
    output$age_paragraph <- shiny::renderUI({
      get_icb_paragraph_age(
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

    # Service
    output$service_paragraph <- shiny::renderUI({
      get_icb_paragraph_service(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })
    output$percent_change_by_service <- shiny::renderPlot({
      plot_percent_change_by_service(
        get_icb_data(),
        measure = input$measure,
        horizon = input$horizon
      )
    })

    # Population usage
    output$population_usage_paragraph <- shiny::renderUI({
      get_icb_paragraph_population_usage(
        get_icb_data(),
        measure = input$measure
      )
    })

    output$count_per_population <- shiny::renderPlot({
      plot_count_per_population(get_icb_data(), measure = input$measure)
    })

    # Patient usage
    output$patient_usage_paragraph <- shiny::renderUI({
      get_icb_paragraph_patient_usage(get_icb_data())
    })

    output$contacts_per_patient <- shiny::renderPlot({
      plot_contacts_per_patient(icb = input$icb)
    })

    # Data Quality
    output$data_quality_paragraph <- shiny::renderUI({
      get_icb_paragraph_dq(get_icb_data(), measure = input$measure)
    })

    output$data_quality_summary_table <- gt::render_gt({
      create_icb_dq_summary_table(get_icb_data(), measure = input$measure)
    })
  })
}
