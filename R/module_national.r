# National module

# UI
national_ui <- function(id) {
  ns <- shiny::NS(id)
  options(scipen = 999)
  bslib::page_fillable(
    bslib::layout_column_wrap(
      fill = FALSE,
      width = 1 / 4,
      height = 100,
      bslib::value_box(
        title = NULL,
        value = glue::glue(
          "{round(get_national_baseline_count('Contacts') / 1000000,1)} million"
        ),
        showcase = bsicons::bs_icon("clipboard-pulse"),
        shiny::p("contacts in 2022/23")
      ),
      bslib::value_box(
        title = NULL,
        value = glue::glue("{get_national_pct_change('Contacts')}% increase"),
        showcase = bsicons::bs_icon("arrow-up-right"),
        shiny::p("in contacts forecast by 2042/43")
      ),
      bslib::value_box(
        title = NULL,
        value = glue::glue(
          "{round(get_national_horizon_count('Contacts') / 1000000,1)} million"
        ),
        showcase = bsicons::bs_icon("graph-up"),
        shiny::p("contacts estimated in 2042/43")
      ),
      bslib::value_box(
        title = NULL,
        value = glue::glue(
          "{round(get_national_baseline_count('Patients') / 1000000,1)} million"
        ),
        showcase = bsicons::bs_icon("people-fill"),
        shiny::p("patients in 2022/23")
      )
    ),
    bslib::layout_columns(
      fill = TRUE,
      column_widths = c(12, 12),
      bslib::navset_card_pill(
        title = "National projections by age group",
        placement = "above",
        full_screen = TRUE,
        bslib::nav_panel(
          title = "Contacts",
          bslib::layout_column_wrap(
            width = NULL,
            style = bslib::css(grid_template_columns = "1fr 3fr"),
            height = 300,
            bslib::card(
              shiny::htmlOutput(ns("national_contacts_sentence"))
            ),
            bslib::card(
              shiny::plotOutput(ns("national_contacts_by_year"))
            )
          )
        ),
        bslib::nav_panel(
          title = "Patients",
          bslib::layout_column_wrap(
            width = NULL,
            style = bslib::css(grid_template_columns = "1fr 3fr"),
            height = 300,
            bslib::card(
              shiny::htmlOutput(ns("national_patients_sentence"))
            ),
            bslib::card(
              shiny::plotOutput(ns("national_patients_by_year"))
            )
          )
        ),
        bslib::nav_panel(
          title = "Data quality",
          bslib::layout_column_wrap(
            width = NULL,
            style = bslib::css(grid_template_columns = "1fr 3fr"),
            height = 300,
            bslib::card(
              shiny::p("Some text about the data quality")
            ),
            bslib::card(
              gt::gt_output(ns("data_quality_summary_table"))
            )
          )
        ),
        footer = bslib::card_body(
          fill = FALSE,
          shiny::includeText("inst/www/warning-note.txt")
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

    output$national_patients_by_year <- shiny::renderPlot({
      plot_national_patients_by_year()
    })

    output$national_contacts_sentence <- shiny::renderUI({
      get_national_sentence("Contacts")
    })

    output$national_patients_sentence <- shiny::renderUI({
      get_national_sentence("Patients")
    })

    output$data_quality_summary_table <- gt::render_gt({
      create_nat_dq_summary_table("Contacts")
    })
  })
}
