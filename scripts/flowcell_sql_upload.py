#!/usr/bin/env python

"""Script replacing flowcell_summary_uppload_LIMS.py
Gets data from the sequencing step and uploads it to statusdb.

Denis Moreno, Science for Life Laboratory, Stockholm, Sweden.
"""

import argparse
import os
import yaml

from LIMS2DB.flowcell_sql import create_lims_data_obj, get_sequencing_steps, upload_to_couch
from LIMS2DB.utils import setupServer

from  genologics_sql.utils import get_session 




def main(args):
    db_session=get_session()

    with open(args.conf) as conf_file:
        conf=yaml.load(conf_file)
    couch=setupServer(conf)
    interval="{} hours".format(args.hours)
    seq_steps=get_sequencing_steps(db_session, interval)
    print seq_steps


    for step in seq_steps:
        for udf in step.udfs: 
            if udf.udfname=="Run ID":
                fcid=udf.udfvalue

        lims_data=create_lims_data_obj(db_session, step)
        upload_to_couch(couch,fcid, lims_data)






if __name__=="__main__":
    usage = "Usage:       python flowcell_sql_upload.py [options]"
    parser = argparse.ArgumentParser(description='Upload flowcells lims data to statusdb.', usage=usage)

    parser.add_argument("-a", "--all_flowcells", dest="all_flowcells", action="store_true", default=False, 
    help = "Tries to upload all the data matching the given update frame (-t) into couchDB." )

    parser.add_argument("-t", "--hours", dest="hours", default=24, type=int, 
    help="Runs older than t hours are not updated. Default is 24 hours.")

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    args = parser.parse_args()
    main(args)


