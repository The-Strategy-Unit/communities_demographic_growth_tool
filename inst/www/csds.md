Baseline community activity is those community contacts in 2022/23 as recorded in the CSDS. The CSDS data underwent several processing steps before being used in this tool.

-  Duplicates were removed where there was both a primary and refresh submission. The latest submission (whether primary or refresh) is the data used in the tool. 

-  Only contacts where the patient was seen are included. This translates to only including those contacts whose attendance status is `Attended on time (5)` or `Arrived late but was seen (6)`. Contacts are excluded where the attendance status indicates a patient’s contact was cancelled or DNAd. Contacts were also excluded if no valid attendance status was recorded. 

-  Data deemed as poor quality were also excluded although these were small in magnitude. Data quality exclusions were as follows:  
    - Person_ID was `NULL`  
    - Sex/Gender was `NULL` or `‘Unknown’ ` 
    - Age was `NULL` or >115 years  
    - Local Authority was `NULL`  
    - Local Authority was not in England  
