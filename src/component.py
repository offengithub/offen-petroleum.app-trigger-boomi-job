"""
Template Component main class.

"""
import csv
import logging
from datetime import datetime
import requests
import json
import os 


from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_USERNAME = 'username'
KEY_PASSWORD = 'password'
KEY_PROCESS_ID = 'process_id'
KEY_ATOM_ID = 'atom_id'
KEY_URL = 'url'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME,KEY_PASSWORD,KEY_PROCESS_ID,KEY_ATOM_ID, KEY_URL]
REQUIRED_IMAGE_PARS = []


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

    # Check the response
    if response.status_code == 200:
        print("Request successful")
        return response.text
    else:
        print("Request failed with status code:", response.status_code)
        return None

class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self) -> None:

        """Runs the component.

        Validates the configuration parameters and triggers a Boomi job.
        """

       # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        
        username = self.configuration.parameters.get(KEY_USERNAME)
        password = self.configuration.parameters.get(KEY_PASSWORD)
        process_id = self.configuration.parameters.get(KEY_PROCESS_ID)
        url = self.configuration.parameters.get(KEY_URL)
        atom_id = self.configuration.parameters.get(KEY_ATOM_ID)

        
        response=trigger_job(url,username, password, process_id, atom_id)
        if response:
            print(response)
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
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
