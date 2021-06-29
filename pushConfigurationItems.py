import argparse
from datetime import datetime, timezone
import json
import pytz
import os
import logging
import traceback
from CommonUtils import CommonRequests
import re
import time

class Snow(object):
    def __init__(self, run_args):
        self.folder_directory = os.getcwd() + "/dump/" + run_args.folder_name

        # Create Common Parameters updated afterwards

        # Create parameters
        self.snow_username = run_args.snow_username
        self.snow_password = run_args.snow_password
        self.snow_data_source_endpoint = run_args.snow_url + "/api/now/import/x_sul_sumo_logic_sg_sumo_logic_ds/insertMultiple"
        self.snow_timezone = run_args.snow_timezone
        self.snow_vcenter_uuid = run_args.snow_vcenter_uuid

        # Common Utils classes
        self.common_requests = CommonRequests()

    # Main Process
    def run(self):
        self.insert_record()

    def insert_record(self):
        logging.debug(str(datetime.now()) + " Pushing Records to Service Now!")
        headers = {"Content-Type":"application/json","Accept":"application/xml"}
        folder = self.folder_directory
        if os.path.exists(folder) and os.path.isdir(folder):
            if os.listdir(folder):
                for filename in os.listdir(folder):
                    # Using readline()
                    if filename.endswith(".txt"):
                        with open(os.path.join(folder,filename), 'r') as file:
                            # Get next line from file
                            count = 0
                            while True:
                                count += 1
                                line = file.readline()
                                if not line:
                                    break
                                else:
                                    content = self.extract_data(line)
                                    if content and len(content) > 0:
                                        for data in content:
                                            data = json.dumps(data)
                                            if data == "Invalid" or data == None:
                                                continue
                                            response = self.common_requests.make_request(self.snow_data_source_endpoint, "post", headers=headers, data=data,
                                                                                    auth=(self.snow_username, self.snow_password))
                                            # Check for HTTP codes other than 202
                                            if response == "":
                                                exit()

                                if count >= 10000:
                                    time.sleep(60)
                                    count = 0
            else:
                print(str(datetime.now()) + " Destination directory is empty:" + folder)
                logging.info(str(datetime.now()) + " Destination directory is empty:" + folder)
        else:
                print(str(datetime.now()) + " Destination directory does not exist:" + folder)
                logging.info(str(datetime.now()) + " Destination directory does not exist:" + folder)

        return None

    def extract_data(self, line):
        if (True if re.search('"awsRegion"', line) else False):
            return self.extract_aws_data(line)
        elif (True if re.search('vm=(.+?),,,', line) else False):
            return self.extract_vmware_data(line)

    def extract_aws_data(self, line):
        try:
            error = 0
            content = []
            lineJson = json.loads(line)
            if lineJson and 'responseElements' in lineJson and lineJson['responseElements'] is not None:
                for instance in lineJson['responseElements']['instancesSet']['items']:
                    data = {}
                    data['vm_name'] = re.search('(.*?)\.', instance['privateDnsName'])[1] if re.search('(.*?)\.', instance['privateDnsName']) else ""
                    data['vm_id'] = instance['instanceId']
                    data['server'] = ""
                    data['datacenter_name'] = ""
                    data['datacenter_id'] = ""
                    data['service_account_type'] = "cmdb_ci_aws_datacenter"
                    data['service_account_id'] = lineJson['userIdentity']['accountId']
                    volumesize = None
                    if  instance['blockDeviceMapping'] and instance['blockDeviceMapping']['items']:
                        volumesize = 0
                        for disk in instance['blockDeviceMapping']['items']:
                            volumesize = volumesize + int(disk['ebs']['volumeSize'])
                    if volumesize:
                        data['vm_disk_space_gb'] = str(volumesize)
                    else:
                        data['vm_disk_space_gb'] = ""
                    networkInterfaces = None
                    if instance['networkInterfaceSet'] and instance['networkInterfaceSet']['items']:
                        networkInterfaces = []
                        for nic in instance['networkInterfaceSet']['items']:
                            networkInterfaces.append(nic['networkInterfaceId'])
                    if networkInterfaces:
                        data['vm_network_adaptors'] = networkInterfaces
                    else:
                        data['vm_network_adaptors'] = []
                    data['datacenter_region'] = lineJson['awsRegion']
                    data['vm_fqdn'] = instance['privateDnsName']
                    if (lineJson['eventName'] == 'RunInstances'):
                        data["vm_state"] = "on"
                    elif (lineJson['eventName'] == 'StopInstances'):
                        data["vm_state"] = "terminated"
                    else:
                        data["vm_state"] = ""
                    source_recency_timestamp = datetime.strptime(lineJson['eventTime'], "%Y-%m-%dT%H:%M:%SZ")
                    if source_recency_timestamp:
                        source_recency_timestamp = source_recency_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        data['source_recency_timestamp'] = source_recency_timestamp

                    if not data['datacenter_region'] or not data['vm_name'] or not data['vm_id'] or not data['vm_fqdn'] or not data['service_account_id'] or not data['source_recency_timestamp']:
                        logging.error("Invalid Data: Please check event types. Required keys are missing.")
                        if error > 100:
                            print(str(datetime.now()) + " Too Many Errors, please check log file.")
                            exit()
                        error += error
                        return []
                    content.append(data)
            return content

        except BaseException as exception:
            print("Exception while creating request Data: %s" % exception)
            print(exception)
            logging.error("Exception while creating request Data: %s" % exception)
            traceback.print_exc()
            return None

    def extract_vmware_data(self, line):
        try:
            error = 0
            content = []
            data = {}
            data['vm_name'] = re.search('vm=(.+?),,,', line)[1] if re.search('vm=(.+?),,,', line) else ""
            data['vm_id'] = re.search('vmMoref=(.+?),,,', line)[1] if re.search('vmMoref=(.+?),,,', line) else ""
            data['server'] = re.search('host=(.+?),,,', line)[1] if re.search('host=(.+?),,,', line) else ""
            data['datacenter_name'] = re.search('datacenter=(.+?),,,', line)[1] if re.search('datacenter=(.+?),,,', line) else ""
            event_type = re.search('eventType=(.+?),,,', line)[1] if re.search('eventType=(.+?),,,', line) else ""
            data['datacenter_id'] = re.search('datacenterMoref=(.+?)$', line)[1] if re.search('datacenterMoref=(.+?)$', line) else ""
            data['service_account_type'] = "cmdb_ci_vcenter_datacenter"
            if self.snow_vcenter_uuid and self.snow_vcenter_uuid != 'None':
                data['service_account_id'] = self.snow_vcenter_uuid
            else: # Try to Extract vCenter UUID from logs.
                #data['service_account_id'] = "vCenterUUID=(.+?)"
                data['service_account_id'] = "09613ad0-45da-11eb-b378-0242ac130007"
            # datacenter_id = re.search('datacenterMoref=(.+?),,,', line)
            # service_account_id = "vCenterUUID=(.+?)"
            data['vm_disk_space_gb'] = ""
            data['vm_network_adaptors'] = ""
            data['datacenter_region'] = ""
            data['vm_fqdn'] = ""

            source_recency_timestamp = datetime.strptime(re.search('(.+?) ,,, ', line)[1], "%Y-%m-%d %H:%M:%S.%f %z") if re.search('(.+?) ,,, ', line) else None
            if source_recency_timestamp:
                if (self.snow_timezone):
                    source_recency_timestamp = source_recency_timestamp.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone(self.snow_timezone))
                source_recency_timestamp = source_recency_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                data['source_recency_timestamp'] = source_recency_timestamp

            if (re.search('VmCreatedEvent', line) and (re.search('VmCreatedEvent', line)[0] in event_type)) or (re.search('VmClonedEvent', line) and (re.search('VmClonedEvent', line)[0] in event_type)):
                data['vm_state'] = "on"
            elif re.search('VmRemovedEvent', line) and (re.search('VmRemovedEvent', line)[0] in event_type):
                data['vm_state'] = "terminated"
            else:
                data['vm_state'] = ""

            if not data['datacenter_name'] or not data['vm_name'] or not data['vm_id'] or not data['server'] or not data['datacenter_id'] or not data['service_account_id']:
                logging.error("Invalid Data: Please check event types. Required keys are missing.")
                if error > 100:
                    print(str(datetime.now()) + " Too Many Errors, please check log file.")
                    exit()
                error += error
                return []
            content.append(data)
            return content

        except BaseException as exception:
            logging.error("Exception while creating VMware request Data: %s" % exception)
            traceback.print_exc()

def main():
    start_time = datetime.now()
    try:
        logging.basicConfig(filename='push_items_to_snow.log', level=logging.INFO)

        logging.info(str(datetime.now()) + "************************ Pushing Data to ServiceNow : Start ************************")
        print(str(datetime.now()) + "************************ Pushing Data to ServiceNow : Start ************************")
        # Get the arguments for Access Key, Access ID, Query, Full Folder path and Sumo Environment to Get the records
        parser = argparse.ArgumentParser()
        parser.add_argument("-u", "--snow_username", dest="snow_username", required=True,
                            help="ServiceNow Username (required).")
        parser.add_argument("-p", "--snow_password", dest="snow_password", required=True,
                            help="ServiceNow Password(required).")
        parser.add_argument("-f", "--folder_name", dest="folder_name", required=True,
                            help="Folder Name to read the the files.")
        parser.add_argument("-c", "--snow_data_source_cid", dest="snow_data_source_cid", required=True,
                            help="ServiceNow Sumo Logic Data Source CID.")
        parser.add_argument("-su", "--snow_url", dest="snow_url", required=True,
                            help="ServiceNow Base URL.")
        parser.add_argument("-tz", "--snow_timezone", dest="snow_timezone", required=True,
                            help="Timezone of events.")
        parser.add_argument("-vu", "--snow_vcenter_uuid", dest="snow_vcenter_uuid", required=False,
                            help="vCenter UUID (Optional).")

        args = parser.parse_args()

        # Local Parameters
        snow = Snow(args)
        snow.run()

    except BaseException as exception:
        print(exception)
        traceback.print_exc()
    finally:
        logging.info(str(datetime.now()) + "********************** Import : Complete ***********************")
        print(str(datetime.now()) + "********************** Service Now Import : Complete ***********************")
        logging.info(str(datetime.now()) + "********************** TIME ELAPSED = " + str(datetime.now() - start_time))


if __name__ == '__main__':
    main()
