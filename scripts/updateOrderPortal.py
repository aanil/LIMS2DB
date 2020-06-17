""" Script to update projects from LIMS
    to corresponding orders in Order Portal."""
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics_sql import queries
from genologics_sql.utils import *
from datetime import *
import json
import argparse
import requests
import os
import LIMS2DB.utils as lutils

class Order_Portal_APIs(object):

    def __init__(self, url, headers, log):

        self.base_url = url
        self.headers = headers
        self.log = log
        self.lims = Lims(BASEURI, USERNAME, PASSWORD)

    def update_order_internal_id(self, open_date, dry_run):
        pjs = self.lims.get_projects(open_date=open_date.strftime("%Y-%m-%d"))
        for project in pjs:
            if project.open_date:
                project_ngi_identifier=project.id
                project_ngi_name=project.name
                try:
                    ORDER_ID=project.udf['Portal ID']
                except KeyError:
                    continue

                url = "{base}/api/v1/order/{id}".format(base=self.base_url, id=ORDER_ID)
                data = {'fields': {'project_ngi_name': project.name, 'project_ngi_identifier': project.id}}
                if not dry_run:
                    response = requests.post(url, headers=self.headers, json=data)
                    assert response.status_code == 200, (response.status_code, response.reason)

                    self.log.info('Updated internal id for order:{} - {}'.format(ORDER_ID, project.id))
                else:
                    print('{} Dry run: Updated internal id for order:{} - {}'.format(ORDER_ID, project.id))

    def update_order_status(self, to_date, dry_run):
        lims_db = get_session()
        pjs = queries.get_last_modified_projectids(lims_db, "24 hours")
        yesterday = (to_date-timedelta(days=1)).strftime("%Y-%m-%d")
        today = to_date.strftime("%Y-%m-%d")
        for p in pjs:
            project = Project(self.lims, id=p)
            try:
                ORDER_ID=project.udf['Portal ID']
            except KeyError:
                continue
            url = "{base}/api/v1/order/{id}".format(base=self.base_url, id=ORDER_ID)
            response = requests.get(url, headers=self.headers)
            data = ''
            try:
                data = response.json()
            except ValueError: #In case a portal id does not exit on lims, skip the proj
                continue
            url = ''
            status = ''

            if (data['status'] == 'accepted' and
                    project.udf.get('Queued') and
                    not project.close_date):
                url = data['links']['processing']['href']
                status_set = 'processing'
            if data['status'] == 'processing':
                if project.udf.get('Aborted') and (project.udf.get('Aborted') == today or project.udf.get('Aborted') == yesterday):
                    url = data['links']['aborted']['href']
                    status_set = 'aborted'
                elif project.close_date and (project.close_date == today or project.close_date == yesterday):
                    url = data['links']['closed']['href']
                    status_set = 'closed'
            if url:
                if not dry_run:
                    #Order portal sends a mail to user on status change
                    response = requests.post(url, headers=self.headers)
                    assert response.status_code == 200, (response.status_code, response.reason)
                    self.log.info('Updated status for order {} from {} to {}'.format(ORDER_ID, data['status'], status_set))
                else:
                    print('Dry run: Updated status for order {} from {} to {}'.format(ORDER_ID, data['status'], status_set))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', metavar="Path to config file", help='Path to config file with URL and API key in JSON format.')
    parser.add_argument('option', metavar="Option to update order",
        help='Choose which order fields to update, either OrderStatus or OrderInternalID')
    parser.add_argument("-l", "--log", dest="logfile",
        default=os.path.join(os.environ['HOME'],'log/LIMS2DB','OrderPortal_update.log'),
        help = 'log file.  Default: ~/log/LIMS2DB/OrderPortal_update.log')
    parser.add_argument('-d', '--dryrun',
                      action="store_true", dest="dryrun", default=False,
                      help='dry run: no changes stored')
    args = parser.parse_args()
    log = lutils.setupLog('orderlogger', args.logfile)

    with open(args.config) as config_file:
        creds = json.load(config_file)
    OP_BASE_URL = creds['OrderPortal'].get('URL')  # Base URL for your OrderPortal instance.
    API_KEY = creds['OrderPortal'].get('API_KEY')  # API key for the user account.
    headers = {'X-OrderPortal-API-key': API_KEY}
    ord_port_apis = Order_Portal_APIs(OP_BASE_URL, headers, log)

    if args.option == 'OrderStatus':
        ord_port_apis.update_order_status(date.today(), args.dryrun)
    elif args.option == 'OrderInternalID':
        ord_port_apis.update_order_internal_id(date.today()-timedelta(days=1), args.dryrun)
