app_server = function(input, output, session) {
  get_icb_data <- shiny::reactive({
    get_all_icbs_data() |>
      dplyr::filter(.data$icb22cdh == input$icb)
  })

  output$national_contacts_by_year <- shiny::renderPlot({
    plot_national_contacts_by_year()
  })

  output$icb_contacts_by_year <- shiny::renderPlot({
    plot_icb_contacts_by_year(get_icb_data())
  })

  output$percent_change_by_age <- shiny::renderPlot({
    plot_percent_change_by_age(get_icb_data())
  })

  output$contacts_per_population <- shiny::renderPlot({
    plot_contacts_per_population(get_icb_data())
  })
}
