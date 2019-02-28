
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from datetime import *
import json
import argparse
# Third-party package: http://docs.python-requests.org/en/master/
import requests

# Variables whose values must be changed for your site:
def main(args):
    with open(args.config) as config_file:
        creds = json.load(config_file)
    OP_BASE_URL = creds['OrderPortal'].get('URL')  # Base URL for your OrderPortal instance.
    API_KEY = creds['OrderPortal'].get('API_KEY')  # API key for the user account.
    headers = {'X-OrderPortal-API-key': API_KEY}

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    yesterday=date.today()-timedelta(days=1)
    pjs=lims.get_projects(open_date=yesterdays.strftime("%Y-%m-%d"))
    for project in pjs:
        if project.open_date:
            project_ngi_identifier=project.id
            project_ngi_name=project.name
            try:
                ORDER_ID=project.udf['Portal ID']
            except KeyError:
                continue
            url = "{base}/api/v1/order/{id}".format(base=OP_BASE_URL, id=ORDER_ID)
            data = {'fields': {'project_ngi_name': project.name, 'project_ngi_identifier': project.id}}
            response = requests.post(url, headers=headers, json=data)
            assert response.status_code == 200, (response.status_code, response.reason)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Update fields in Order Portal')
    parser.add_argument('config', metavar="Link to config file", help='Link to config file with URL and API key in JSON format.')
    args = parser.parse_args()


    main(args)
