from datetime import datetime
import os
import requests
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class CommonRequests(object):

    def make_request(self, url, method="get", **args):
        start_time = datetime.now()
        sess = self.get_new_session()
        try:
            response = getattr(sess, method)(url, timeout=20, **args)
            if 400 <= response.status_code < 600:
                response.reason = response.text if response.text else response.reason
            logging.debug(str(datetime.now()) + " IN MAKE REQUEST METHOD :- MethodType: %s Request url: %s, status_code: %s."
                          % (method, url, response.status_code))
            response.raise_for_status()
            if response.status_code == 200:
                data = response.json() if len(response.content) > 0 else {}
                return data
            else:
                logging.info(str(datetime.now()) + " IN MAKE REQUEST METHOD :- Request Completed: %s, url: %s, Status: %s, Reason: %s." % (
                    response.content, url, response.status_code, response.reason))
                return response.content
        except requests.exceptions.HTTPError as error:
            logging.error("IN MAKE REQUEST METHOD :- Http Error: %s" % error)
        except requests.exceptions.ConnectionError as error:
            logging.error("IN MAKE REQUEST METHOD :- Connection Error: %s" % error)
        except requests.exceptions.Timeout as error:
            time_elapsed = datetime.now() - start_time
            logging.error("IN MAKE REQUEST METHOD :- Timeout Error: %s with time: %s." % (error, time_elapsed))
        except requests.exceptions.RequestException as error:
            logging.error("IN MAKE REQUEST METHOD :- Error: %s" % error)
        finally:
            sess.close()
            logging.debug(str(datetime.now()) + " IN MAKE REQUEST METHOD :- Rest API Call complete, Closing Session.")
        return ""

    def get_new_session(self, max_retry=3, back_off_factor=0.1):
        logging.debug(str(datetime.now()) + " IN CREATE SESSION :- Session Create Start")
        retried_status_codes = [502, 503, 504, 429]
        sess = requests.Session()
        retries = Retry(total=max_retry, backoff_factor=back_off_factor, status_forcelist=retried_status_codes)
        sess.mount('https://', HTTPAdapter(max_retries=retries))
        sess.mount('http://', HTTPAdapter(max_retries=retries))
        logging.debug(str(datetime.now()) + " IN CREATE SESSION :- Session Create Complete")
        return sess


class FileProcessing(object):

    def __init__(self, folder_directory, date_format="%Y-%m-%dT%H:%M:%SZ"):
        self.folder_directory = folder_directory
        self.date_format = date_format

    def update_data_in_file(self, filename, content):
        logging.debug(str(datetime.now()) + " Provided file name is " + filename)
        if not os.path.exists(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'a') as f:
            f.write('\n'.join(content))
            f.write('\n')


    def files_as_per_file_size(self, original_file_name, new_file_name, divide_size=10):
        logging.debug(str(datetime.now()) + " IN FILES AS PER FILE SIZE :- Creating Multiple files for %s." % original_file_name)
        file_names = self.bytesize_chunking(original_file_name, new_file_name, divide_size)
        os.remove(self.folder_directory + "/" + original_file_name)
        logging.debug(str(datetime.now()) + " IN FILES AS PER FILE SIZE :- Metric Dump Complete with File location %s." % original_file_name)
        return file_names

    def bytesize_chunking(self, original_file_name, new_file_name, divide_size=25):
        size = int(os.stat(self.folder_directory + "/" + original_file_name).st_size / 1000000)
        sections = int(size / divide_size) + 1
        records = []
        file_names = set()
        with open(self.folder_directory + "/" + original_file_name) as src:
            records.extend(src.read().splitlines())

        number_of_records_in_one_file = len(records) // sections

        new_records = self.batchsize_chunking(records, number_of_records_in_one_file)
        counter = 1
        for record in new_records:
            filename = "replaylogs/" + new_file_name + "_" + str(counter) + ".txt"
            file_full_path = self.folder_directory + "/" + filename
            counter += 1
            self.update_data_in_file(file_full_path, record)
            file_names.add(filename)
        return file_names

    def batchsize_chunking(self, iterable, size=1):
        length = len(iterable)
        for idx in range(0, length, size):
            data = iterable[idx:min(idx + size, length)]
            yield data

class TimeProcessing(object):

    def convert_utc_date_to_epoch(self, date_str, date_format='%d/%m/%Y %H:%M:%S %p', milliseconds=True):
        epoch = datetime(1970, 1, 1)
        timestamp = (datetime.strptime(date_str, date_format) - epoch).total_seconds()
        if milliseconds:
            timestamp = timestamp * 1000
        return int(timestamp)
