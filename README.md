# sumologic-servicenow-cmdb-ci-sync
## Historical Import 

This section provides information to import historical data from Sumo Logic into ServiceNow. 
*Note: For Vmware, the import will only work if the data has been imported into Sumo Logic using this script release or a later release.*

The historical import script queries Sumo Logic based on the input parameters and stores the VMWare/AWS CloudTrail events in files, then the events are imported into ServiceNow by calling ServiceNow REST APIs.

To import historical data, follow the below steps on a Unix machine:

* Clone the script:
  * git clone https://github.com/SumoLogic/sumologic-servicenow-cmdb-ci-sync.git 
* Edit the file sumo_snow_historical_import.sh, set the parameters as explained in the file.

* Execute the script by running the below command:
    *sh sumo_snow_historical_import.sh*

* Verify the files generated in the specified folder and records created in ServiceNow.

## Historical Import Troubleshooting 
If the configuration items are not being created in ServiceNow:
* Verify the Sumo Logic and ServiceNow credentials.
* Verify the data files containing events information generated in the folder specified.
* Make sure that the query returns the results for the specified time range. This can be verified from Sumo Logic UI.
* Review the log files:
  * fetch_logs_from_sumo.log: This file contains the logs generated when extracting the data from Sumo Logic.
  * push_items_to_snow.log: This file contains the logs generated when pushing the data to ServiceNow.
