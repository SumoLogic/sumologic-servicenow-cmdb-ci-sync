#!/bin/bash

# Parameters - provide values
# Sumo Logic enviornment value like api.sumologic.com or api.<deployment>.sumologic.com. Deployment include au, ca, de, eu, fed, in, jp and us2.
export sumologic_environment=api.sumologic.com
# Sumo Logic Access ID. Can be obtained from account page.
export access_id='<SUMOLOGIC_ACCESS_ID>'
# Sumo Logic Access Key. Can be obtained from account page.
export access_key='<SUMOLOGIC_ACCESS_KEY>'
# provide a query as per your interest
export query='_sourceCategory = Labs/VMWareSNOW and (\"VmCreatedEvent\" or \"VmClonedEvent\"  or \"VmRemovedEvent\")'
# export query='(_sourceCategory = *cloudtrail*) AND _sourceName = s3objectaudit* | json field=_raw \"eventName\"| where (eventName = \"RunInstances\" or eventName =\"StopInstances\")'
# Time format is in GMT. Follow format as Date/Month/Year Hour:Minute:Seconds PM|AM
export sumo_query_timerange='06/05/2021 00:00:00 AM to 06/05/2021 12:00:00 PM'
# Provide a file name to create without any extension like .txt or .log.
export file_name=sumo_snow_sg
# Provide a folder name.
export folder_name=sumo_snow

# Service Now parameters.
# This flag can be used to prevent the extracted data being pushed to ServiceNow.
export push_data_to_servicenow=true
# Base URL of your Service Now instance.
export snow_url='<SERVICENOW_BASE_URL>' # Example 'https://ven01365.service-now.com'
# Service Now Username
export snow_username='<SERVICENOW_USERNAME>'
# Service Now Password
export snow_password='<SERVICENOW_PASSWORD>'
# Set the events timezone. Only required for Vmware events.
export snow_timezone='America/Los_Angeles'

python3 fetchLogDump.py -k ${access_id} -c ${access_key} -e ${sumologic_environment} -q "${query}" \
 -f ${folder_name} -fn ${file_name} -s "${sumo_query_timerange}"

if [[ "${push_data_to_servicenow}" == "true" ]]
then
  python3 pushConfigurationItems.py -u ${snow_username} -p ${snow_password} \
   -su "${snow_url}" -f ${folder_name} -tz ${snow_timezone}
fi