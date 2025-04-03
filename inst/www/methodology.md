## About

The community services demographic growth tool provides projections for the volume of future community contacts necessary to keep up with demand due to population change over the coming decades.  

The projections shown relate solely to the pressures from population size and ageing. They do not account for other factors influencing demand for community contacts - such as left-shift from acute settings or changes to service provision within community services themselves. 

The tool was built by [The Strategy Unit](https://www.strategyunitwm.nhs.uk/).


## Methodology

We establish the rate of care per patient (attended contacts per patient) for patients who have been in contact with community services during the baseline year. We then project future community contacts, through to 2042/43, using the population growth expected by age, gender and geography (Local Authority).  

Population projections are for calendar years but NHS planning and commissioning cycles are usually based on financial years. Therefore, we estimated financial year population projections from calendar years. For each financial year, this allocated 75% of the population from the first calendar year and 25% of the population from the second calendar year. 

As the projection of community contacts was based on how the population was projected to change by age, gender and local authority, it was important not to duplicate patients where they may have appeared multiple times. For example, a patient with contacts both before and after their birthday would be counted twice under two ages – falsely lowering the rate of care estimates. To counter this, for the factors of age and gender only one value was assigned to patient for the duration of the baseline year. For age, the age at first contact in 2022/23 was used. 

## Community Services Dataset (CDSD)

Baseline community activity is those community contacts in 2022/23 as recorded in the CSDS. The CSDS data underwent several processing steps before being used in this tool.

-  Duplicates were removed where there was both a primary and refresh submission. The latest submission (whether primary or refresh) is the data used in the tool. 

-  Only contacts where the patient was seen are included. This translates to only including those contacts whose attendance status is `Attended on time (5)` or `Arrived late but was seen (6)`. Contacts are excluded where the attendance status indicates a patient’s contact was cancelled or DNAd. Contacts were also excluded if no valid attendance status was recorded. 

-  Data deemed as poor quality were also excluded although these were small in magnitude. Data quality exclusions were as follows:  
    - Person_ID was `NULL`  
    - Sex/Gender was `NULL` or `‘Unknown’ ` 
    - Age was `NULL` or >115 years  
    - Local Authority was `NULL`  
    - Local Authority was not in England  

## Provider exclusions

It is generally recognised that there are issues with CSDS data concerning the recording of attendance status (which was not mandatory before January 2023) and the regular submission of data. To minimise the impact of poor-quality data, a subset of providers were identified as having characteristics that would suggest they were of “consistent” quality. 

Providers were selected for inclusion in the “consistent” providers list if they had 12 submissions for 2022/23 (i.e. they submitted for every month of the year) as well as submitting contacts marked as attended in all 12 months of the year. This gave a list of 111 “consistent” providers, compared to 212 providers in the full data set. This smaller data set, whilst having just over half of the providers, did cover 79% of attended community contacts - 54,944,338 community contacts compared to 69,483,247 when using all providers. 

Since only the subset of community contacts from “consistent” providers are included in this tool absolute volumes of community contacts are undercounted. However, given the subset of data used here is large (79%), it can be viewed as a representative sample for England. This means that where measures are percentage-based (rather than absolute numbers) ICB and England projections are valid.  


## Population projections data

The source of the population projection data was [ONS subnational population projections for England: 2018-based](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/2018based) by local authority, age and sex. The population projections used was the principal projection and provided populations for each calendar year from 2018 to 2043. 