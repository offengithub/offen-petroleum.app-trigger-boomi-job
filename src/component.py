"""
Template Component main class.

"""
import csv
import logging
from datetime import datetime
import requests
import json
import os 
import logging
import requests
import xml.etree.ElementTree as ET
from keboola.component.exceptions import UserException
from keboola.component.base import ComponentBase
import json 
import xmltodict
from datetime import datetime, timezone, timedelta
import time
from math import ceil 




current_time=datetime.now(timezone.utc)

# configuration variables
KEY_USERNAME = 'username'
KEY_PASSWORD = 'password'
KEY_PROCESS_ID = 'process_id'
KEY_ATOM_ID = 'atom_id'
KEY_JOB_STATUS_URL = 'job_status_url'
KEY_JOB_TRIGGER_URL = 'job_trigger_url'
KEY_POLL_FREQUENCY ='poll_frequency'
KEY_WEBHOOK_URL ='webhook_url'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME,KEY_PASSWORD,KEY_PROCESS_ID,KEY_ATOM_ID, KEY_JOB_STATUS_URL,KEY_JOB_TRIGGER_URL ]
REQUIRED_IMAGE_PARS = []


def post_to_teams(webhook_url, message):
    """
    Post a message to Microsoft Teams.
    :param webhook_url: The webhook URL provided by Microsoft Teams.
    :param message: The message to be posted.
    """
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        'text': message
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        raise ValueError(f"Request to Teams returned an error {response.status_code}, the response is:\n{response.text}")



def trigger_job(url: str,username: str, password: str, process_id: str, atom_id: str):
    """Triggers a Boomi job.

  Args:
    url: The URL of the Boomi API endpoint.
    username: The Boomi username.
    password: The Boomi password.
    process_id: The ID of the Boomi process to trigger.
    atom_id: The ID of the Boomi atom to trigger.

  Returns:
    The response text from the Boomi API.
  """

    # Define the API endpoint and URL
    url = url

    # Define the XML request body
    xml_request_body = f"""
    <ExecutionRequest processId="{process_id}" atomId="{atom_id}" xmlns="http://api.platform.boomi.com/">
    </ExecutionRequest>
    """

    # Define headers with Basic Authentication
    headers = {
        "Content-Type": "application/xml"
    }
    auth = (username, password)

    # Send the HTTP POST request
    response = requests.post(url, headers=headers, data=xml_request_body, auth=auth)

    # Check the response status code
    if response.status_code != 200:
        logging.error("Request failed with status code: %s, response: %s", response.status_code, response.text)
        raise UserException(f"Failed to trigger the Boomi job, response code: {response.status_code}")

    # Parse the XML response
    try:
        root = ET.fromstring(response.text)
        # Check if the response contains ExecutionRequest and requestId
        if root.tag.endswith('ExecutionRequest') and 'requestId' in root.attrib:
            logging.info("Job triggered successfully with requestId: %s", root.attrib['requestId'])
        else:
            raise UserException("Boomi API did not return a success message.")
    except ET.ParseError as e:
        logging.error("Failed to parse XML response: %s", e)
        raise UserException("Failed to parse the Boomi API response.")
    
    # Convert XML to a Python dictionary
    dict_data = xmltodict.parse(response.text)

    # Convert the Python dictionary to a JSON string
    response_json_data = json.dumps(dict_data, indent=4)

    return response_json_data


def check_job_status(url: str,username: str, password: str, process_id: str, atom_id: str, start_time = (current_time - timedelta(days=1)).isoformat(), 
                           end_time=current_time.isoformat()):
    """
    Queries the execution status of a Snowflake integration process in Boomi Atomsphere.

    :param username: The username for basic authentication.
    :param password: The password for basic authentication.
    :param process_id: The unique identifier for the process.
    :param atom_id: The unique identifier for the atom.
    :param start_time: The start time for the query range (ISO 8601 format).
    :param end_time: The end time for the query range (ISO 8601 format).
    :param poll_frequency: How frequent do you want to check the job for status
    :return: The response from the API.
    """

    # The URL for the API endpoint
    url = url

    # Construct the XML body for the request
    raw_xml = f"""
    <QueryConfig xmlns="http://api.platform.boomi.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <QueryFilter>
            <expression operator="and" xsi:type="GroupingExpression">
                <nestedExpression operator="BETWEEN" property="executionTime" xsi:type="SimpleExpression">
                    <argument>{start_time}</argument>
                    <argument>{end_time}</argument>
                </nestedExpression>
                <nestedExpression operator="EQUALS" property="processId" xsi:type="SimpleExpression">
                    <argument>{process_id}</argument>
                </nestedExpression>
                <nestedExpression operator="EQUALS" property="atomId" xsi:type="SimpleExpression">
                    <argument>{atom_id}</argument>
                </nestedExpression>
            </expression>
        </QueryFilter>
    </QueryConfig>
    """

    # Perform the POST request with Basic Auth
    response = requests.post(url, auth=(username, password), headers={'Content-Type': 'application/xml'}, data=raw_xml.strip())

    # Check the response status code
    
    if response.status_code != 200:
        logging.error("Request failed with status code: %s, response: %s", response.status_code, response.text)
        raise UserException(f"Failed to trigger the Boomi job, response code: {response.status_code}")
 
    # Convert XML to a Python dictionary
    dict_data = xmltodict.parse(response.text)

    # Convert the Python dictionary to a JSON string
    response_json_data = json.dumps(dict_data, indent=4)

    return response_json_data

class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self) -> None:
        job_status = None

        """Runs the component.
        Validates the configuration parameters and triggers a Boomi job.
        """

       # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)

        #webhook_url = 'https://offenpet.webhook.office.com/webhookb2/863639ee-1bda-4fc3-a3f3-efbc70cf42b2@326321f9-9e95-45c7-8701-8165523a9735/IncomingWebhook/7600ef1aed0049afb4dc1eec2fbbd8a2/f2164844-b26c-481c-b057-65cec085a8dd'
        username = self.configuration.parameters.get(KEY_USERNAME)
        password = self.configuration.parameters.get(KEY_PASSWORD)
        process_id = self.configuration.parameters.get(KEY_PROCESS_ID)
        job_status_url = self.configuration.parameters.get(KEY_JOB_STATUS_URL)
        job_trigger_url = self.configuration.parameters.get(KEY_JOB_TRIGGER_URL)
        atom_id = self.configuration.parameters.get(KEY_ATOM_ID)
        poll_frequency=self.configuration.parameters.get(KEY_POLL_FREQUENCY)
        webhook_url=self.configuration.parameters.get(KEY_WEBHOOK_URL)

        # trigger the job
        
        triger_response =trigger_job(job_trigger_url, username, password, process_id, atom_id)
        if triger_response:
            print('=============================================================== printing job trigger response ===========================================================================')
            print(triger_response)
        else:
            print('Job could not be triggered')
        time.sleep(180)
        

        # Check job status every 10 minutes
        while True:
            current_time = datetime.now()
            status_response= check_job_status(
            job_status_url,
            username,
            password,
            process_id, 
            atom_id
            #start_time=current_time.isoformat(),
            #end_time=current_time.isoformat()
            )
             #(current_time - timedelta(days=1)).isoformat()
            if status_response:
            # Extract the job status
                response_dict = json.loads(status_response)
                formatted_response = json.dumps(response_dict, indent=4)
                print(formatted_response)
                # Accessing the 'bns:result' key which is expected to be a list
                results = response_dict.get('bns:QueryResult', {})
                #print("DATA TYPE RESULTS")
                #print(type(results))
                execution_record = results.get('bns:result', {})[-1]
                #execution_record = results.get('bns:result', {})
                print(type(execution_record))
                #print(len(execution_record))
                #print("EXECUTION RESULTS")
                #print(execution_record)

                # Check if 'execution_record' is indeed a dictionary
                if execution_record and isinstance(execution_record, dict):
                    job_status = execution_record.get('bns:status', 'Unknown Status')
                    job_name = execution_record.get('bns:processName', 'Unknown process name')
                    job_run_time = execution_record.get('bns:executionDuration', 'cant access runtime')
                    if job_status == "COMPLETE":
                        print("Job completed successfully")
                        print(f"job: {job_name} is {job_status}. Runtime:{ceil(float(job_run_time)/1000/60)} minutes")
                        job_status_message=f"job: {job_name} is {job_status}. Runtime:{ceil(float(job_run_time)/1000/60)} minutes"
                        if webhook_url:
                            post_to_teams(webhook_url, job_status_message)
                        break

                    elif job_status == "ERROR":
                        print("Error occurred while checking job status")
                        break
                    # If the job status is not COMPLETE or Error, print a message indicating that the job is still running
                    else:
                        job_status_message=f"Job status: {job_status}. Checking again in 10 minutes."
                        if webhook_url:
                            post_to_teams(webhook_url, job_status_message)
            else:
                print("No status response received. Checking again in 10 minutes.")

            time.sleep(int(poll_frequency))  # Wait for 10 minutes or 600 seconds before checking again
        #self.write_manifest(out_table)

"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception("User configuration error: %s", exc)
        exit(1)
    except Exception as exc:
        logging.exception("Unexpected error: %s", exc)
        exit(2)
