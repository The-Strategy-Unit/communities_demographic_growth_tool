# Community Services Demographic Growth Tool

A [Shiny app][tool] that combines Community activity recorded in 2022-23,
from the NHS England Community Services Dataset ([CSDS][csds]), with
[ONS subnational population projections][ons] to 2043,
to show the projected usage of community services.

For this app, projections are calculated by financial year, to match the
basis period of the CSDS data.

The app provides breakdowns of the data:

* by ICB, as well as providing national figures for England
* by age group
* by service (eg Physiotherapy)
* as a rate per individual patient
* as a rate per 1,000 population of the ICB (based on current boundaries).

The app can show activity in terms of number of contacts (appointments etc),
or in terms of the number of individual patients using the services.

## Running the app locally

Clone the package repository and check you have all the required R dependencies
installed (see the DESCRIPTION file for the package list).

Pull down the source data for the tool from the Databricks system using the
functions and steps in `data-raw/export.R`.

(You will need access to Databricks via an auth token).

This should create four folders in the root of the repository:

* icb_contacts_final
* icb_patients_final
* nat_contacts_final
* nat_patients_final

These folders contain the source data for the app in [parquet][pqt] format.

Then you can run the app with:

```r
devtools::load_all()
run_app()
```

This should launch the app in the viewer pane of your IDE, and/or in a browser
tab.

The tool's official deployment is on the SU's [Connect server][tool].

## Notes

It is important to note that the population figures provided by the ONS are
_projections_, not predictions.
This means that they do not take into account potential or expected changes in,
for example, national policy around migration, for example, or the risk of
major events such as pandemics or climatic events.

The projection used is the principal projection.

The app does not take into account non-demographic change such as mitigations;
it is a projection based on 2022/23 recorded activity only.

## Contact

For any questions about using the app, or any technical questions about the
code, the datasets or getting the app running, contact:

* Fran Barton (francis.barton@nhs.net) or
* Rhian Davies (rhian.davies25@nhs.net)


[csds]: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/community-services-data-set
[ons]: https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/previousReleases
[tool]: https://connect.strategyunitwm.nhs.uk/communities_demographic_growth/
[pqt]: https://parquet.apache.org
