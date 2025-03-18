# ICB module

# UI
icb_ui <- function(id) {
  ns <- shiny::NS(id)

  bslib::page_sidebar(
    sidebar = bslib::sidebar(
      shiny::selectInput(
        ns("icb"),
        "Select ICB",
        choices = icb_list()
      ),
      shiny::selectInput(
        ns("horizon"),
        label = "Select horizon year",
        choices = year_list()[-(1:3)], # Only shortest horizon of 3 years
        selected = "2042_43" # TODO Don't hardcode
      ),
      shiny::selectInput(
        ns("measure"),
        label = "Select a measure",
        choices = c("Contacts", "Patients"),
        selected = "Contacts"
      )
    ),
    bslib::layout_column_wrap(
      width = 0.5,
      height = 300,
      bslib::card(
        full_screen = TRUE,
        bslib::card_header("ICB-level contacts projection by financial year"),
        shiny::plotOutput(ns("icb_contacts_by_year"))
      ),
      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Projected % change in total contacts over period, by age group"
        ),
        shiny::plotOutput(ns("percent_change_by_age"))
      ),
      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Contacts per 1000 population, 2022/23, by age group"
        ),
        shiny::plotOutput(ns("contacts_per_population"))
      ),
      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Projected % change in total contacts over period, by service"
        ),
        shiny::plotOutput(ns("percent_change_by_service"))
      )
    )
  )
}


#

# Server

icb_server <- function(id) {
  shiny::moduleServer(id, function(input, output, session) {
    get_icb_data <- shiny::reactive({
      get_all_icbs_data() |>
        dplyr::filter(.data$icb22cdh == input$icb)
    })

    output$icb_contacts_by_year <- shiny::renderPlot({
      plot_icb_contacts_by_year(get_icb_data(), horizon = input$horizon)
    })

    output$percent_change_by_age <- shiny::renderPlot({
      plot_percent_change_by_age(get_icb_data(), horizon = input$horizon)
    })

    output$contacts_per_population <- shiny::renderPlot({
      plot_contacts_per_population(get_icb_data())
    })

    output$percent_change_by_service <- shiny::renderPlot({
      plot_percent_change_by_service(get_icb_data(), horizon = input$horizon)
    })
  })
}
