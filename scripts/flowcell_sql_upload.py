#!/usr/bin/env python

"""Script replacing flowcell_summary_uppload_LIMS.py
Gets data from the sequencing step and uploads it to statusdb.

Denis Moreno, Science for Life Laboratory, Stockholm, Sweden.
"""

import argparse
import os
import yaml
import logging
import logging.handlers

from LIMS2DB.flowcell_sql import create_lims_data_obj, get_sequencing_steps, upload_to_couch
from LIMS2DB.utils import setupServer

from  genologics_sql.utils import get_session 




def main(args):
    #get the session with the lims db
    db_session=get_session()

    #set up a log
    mainlog = logging.getLogger('fsullogger')
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(args.logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)

    #read the configuration
    with open(args.conf) as conf_file:
        conf=yaml.load(conf_file)

    
    couch=setupServer(conf)
    interval="{} hours".format(args.hours)

    #list the right sequencing steps
    seq_steps=get_sequencing_steps(db_session, interval)


    for step in seq_steps:
        for udf in step.udfs: 
            if udf.udfname=="Run ID":
                fcid=udf.udfvalue

        mainlog.info("updating {}".format(fcid))
        #generate the lims_data dict key
        lims_data=create_lims_data_obj(db_session, step)
        #update the couch right couch document
        upload_to_couch(couch,fcid, lims_data)






if __name__=="__main__":
    usage = "Usage:       python flowcell_sql_upload.py [options]"
    parser = argparse.ArgumentParser(description='Upload flowcells lims data to statusdb.', usage=usage)

    parser.add_argument("-a", "--all_flowcells", dest="all_flowcells", action="store_true", default=False, 
    help = "Tries to upload all the data matching the given update frame (-t) into couchDB." )

    parser.add_argument("-t", "--hours", dest="hours", default=24, type=int, 
    help="Runs older than t hours are not updated. Default is 24 hours.")

    parser.add_argument("-l", "--logfile", dest = "logfile", help = ("log file",
                      " that will be used. default is $HOME/lims2db_flowcells.log "), default=os.path.expanduser("~/lims2db_flowcells.log"))

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    args = parser.parse_args()
    main(args)


