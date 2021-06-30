import argparse
import json
import logging
import os
import time
import traceback
from datetime import datetime


from CommonUtils import TimeProcessing, CommonRequests, FileProcessing

class LogDump(object):

    def __init__(self, run_args):
        self.folder_directory = os.getcwd() + "/dump/" + run_args.folder_name

        # Create Common Parameters updated afterwards

        # Create parameters
        self.access_id = run_args.access_id
        self.access_key = run_args.access_key
        self.sumo_logic_environment = run_args.sumo_env
        self.log_query = run_args.log_query
        self.file_name_to_be_created = run_args.file_name

        # Common Utils classes
        self.time_processing = TimeProcessing()
        self.common_requests = CommonRequests()
        self.file_processing = FileProcessing(self.folder_directory)

        # Calculate Epoch Time Range for start and end date
        query_time = run_args.sumo_query_time_range.split(" to ")
        self.from_epoch_time_range = str(self.time_processing.convert_utc_date_to_epoch(query_time[0]))
        self.to_epoch_time_range = str(self.time_processing.convert_utc_date_to_epoch(query_time[1]))

    # Main Process
    def run(self):
        self.create_log_dump()

    # Step 1  - To create Log dump based on query using APIs.
    def create_log_dump(self):
        # Create a search job
        search_job_id = self.search_job_to_create_search_job_id()
        if search_job_id:
            # Check the status of search job id.
            message_count = self.search_job_status_to_get_message_count(search_job_id)
            print(str(datetime.now()) + " Total Messages to be fetched are " + str(message_count))
            logging.info(str(datetime.now()) + " Total Messages to be fetched are " + str(message_count))
            # Get all the messages
            if message_count:
                raw_messages = self.search_job_messages_to_get_messages(search_job_id, message_count)

                filename = self.file_name_to_be_created + ".txt"
                file_full_path = self.folder_directory + "/" + filename
                if os.path.exists(file_full_path):
                    os.rename(file_full_path, file_full_path + "_bkp_" + str(datetime.now()))
                self.file_processing.update_data_in_file(file_full_path, raw_messages)

                logging.info(str(datetime.now()) + " Single File Creation Complete %s." % filename)

                # Step 2: If File size is more than 10 MB, then
                # divide the Single file based on the file sizes.
                divide_size = 10
                if (int(os.stat(self.folder_directory + "/" + filename).st_size / 1000000)) > divide_size:
                    self.file_processing.files_as_per_file_size(filename,
                                                                             self.file_name_to_be_created,
                                                                             divide_size)
                    logging.info(str(datetime.now()) + " Multiple files created from single file.")

    def search_job_to_create_search_job_id(self):
        logging.debug("IN SEARCH JOB API :- Rest API Call Start")
        headers = {'Content-Type': 'application/json'}
        url = "https://" + self.sumo_logic_environment + "/api/v1/search/jobs"
        data = '{"query": "' + self.log_query + '", "from": ' + self.from_epoch_time_range + ', "to": ' \
               + self.to_epoch_time_range + ', "timeZone": "UTC"}'
        response = self.common_requests.make_request(url, "post", headers=headers, data=data,
                                                     auth=(self.access_id, self.access_key))

        if response:
            response = json.loads(response)
        if response and "id" in response:
            logging.debug(str(datetime.now()) + " IN SEARCH JOB API :- Rest API Call Complete")
            return response["id"]
        return None

    def search_job_status_to_get_message_count(self, search_job_id):
        logging.debug(str(datetime.now()) + " IN SEARCH JOB STATUS API :- Rest API Call Start")
        headers = {'Content-Type': 'application/json'}
        url = "https://" + self.sumo_logic_environment + "/api/v1/search/jobs/" + search_job_id

        state = "GATHERING RESULTS"

        while state == "GATHERING RESULTS":
            response = self.common_requests.make_request(url, "get", headers=headers,
                                                         auth=(self.access_id, self.access_key))

            if response and "state" in response:
                logging.debug(str(datetime.now()) + " Current State is " + response["state"])
                if response["state"] == "DONE GATHERING RESULTS":
                    logging.debug(str(datetime.now()) + " IN SEARCH JOB STATUS API :- Rest API Call Complete")
                    return response["messageCount"]
                elif response["state"] != "GATHERING RESULTS":
                    state = "EXIT"
                else:
                    time.sleep(2)
        return None

    def search_job_messages_to_get_messages(self, search_job_id, messages_count):
        logging.debug(str(datetime.now()) +
            " IN SEARCH JOB MESSAGES API :- Rest API Call Start with message count as " + str(messages_count))
        headers = {'Content-Type': 'application/json'}
        url = "https://" + self.sumo_logic_environment + "/api/v1/search/jobs/" + search_job_id + "/messages"

        count = 0
        raw_messages = []
        while count < messages_count:
            params = {'limit': 10000, 'offset': count}
            response = self.common_requests.make_request(url, "get", headers=headers, params=params,
                                                         auth=(self.access_id, self.access_key))

            if response and "messages" in response:
                messages = response["messages"]
                for message in messages:
                    if "map" in message and "_raw" in message["map"]:
                        raw_messages.append(message["map"]["_raw"])
                count = count + len(messages)
                logging.info(str(datetime.now()) + " Successfully fetched Messages as " + str(len(messages))
                             + ". Increment counter to " + str(count))
        logging.debug(str(datetime.now()) + " IN SEARCH JOB MESSAGES API :- Rest API Call Complete with message count as " + str(count))
        return raw_messages


def main():
    start_time = datetime.now()
    try:
        logfile = 'fetch_logs_from_sumo_{}.log'.format(datetime.now().strftime('%d-%m-%Y-%T'))
        logging.basicConfig(filename=logfile, level=logging.INFO)

        logging.info(str(datetime.now()) + "************************ Sumo Logic Service Now Configuration Item Historical Import : ************************")
        print(str(datetime.now()) + "************************ Sumo Logic Service Now Configuration Item Historical Import : ************************")
        logging.info(str(datetime.now()) + "************************ Extracting Data from Sumo Logic : Start ************************")
        print(str(datetime.now()) + "************************ Extracting Data from Sumo Logic : Start ************************")
        # Get the arguments for Access Key, Access ID, Query, Full Folder path and Sumo Environment to Get the records
        parser = argparse.ArgumentParser()
        parser.add_argument("-k", "--access_id", dest="access_id", required=True,
                            help="Access Id for deployment(required).")
        parser.add_argument("-c", "--access_key", dest="access_key", required=True,
                            help="Access key for deployment(required).")
        parser.add_argument("-q", "--log_query", dest="log_query", required=True,
                            help="Log query to fetch all logs.")
        parser.add_argument("-f", "--folder_name", dest="folder_name", required=True,
                            help="Folder Name to create all the the files.")
        parser.add_argument("-fn", "--file_name", dest="file_name", required=True,
                            help="File Name to put the data. No extension like .txt or .json need to be provided.")
        parser.add_argument("-e", "--sumo_env", dest="sumo_env", required=True,
                            help="Sumo Logic Environment")
        parser.add_argument("-s", "--sumo_query_time_range", dest="sumo_query_time_range", required=True,
                            help="Time range for the provided sumo Query.")
        args = parser.parse_args()

        # Local Parameters
        log_dump = LogDump(args)
        log_dump.run()

    except BaseException as exception:
        traceback.print_exc()
    finally:
        logging.info(str(datetime.now()) + "********************** Sumo Logic Export : Complete ***********************")
        print(str(datetime.now()) + "********************** Sumo Logic Export : Complete ***********************")
        logging.info(str(datetime.now()) + "********************** TIME ELAPSED = " + str(datetime.now() - start_time))


if __name__ == '__main__':
    main()
