
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from scilifelab.db.statusDB_utils import load_couch_server

import LIMS2DB.objectsDB.objectsDB as newDB
import scilifelab.lims_utils.objectsDB as oldDB
import datetime
import argparse
import logging
import os
import sys

def validate_cores(args):
    mylims = Lims(BASEURI, USERNAME, PASSWORD)
    pjs=mylims.get_projects()
    couch = load_couch_server(args.conf)
    proj_db = couch['projects']
    samp_db = couch['samples']
    tested=0
    passed=0
    failed=[]
    log=setupLog(args)
    for p in pjs:
        if p.open_date:
            opDate=datetime.datetime.strptime(p.open_date, '%Y-%m-%d')
            now=datetime.datetime.now()

            if now-opDate < datetime.timedelta(days=args.days):
                tested+=1
                log.info("comparing project {0}".format(p.name))
                oldpj= oldDB.ProjectDB(mylims, p.id, samp_db)
                newpj= newDB.ProjectDB(mylims, p.id, samp_db)
                if oldpj.obj == newpj.obj:
                    log.info("passed")
                    passed+=1
                else:
                    log.error("PROJECT {0} FAILED COMPARISON".format(p.name))
                    failed.append(p.name)

            else:
                log.info("skipping project {0}, too old".format(p.name))
        else:
            log.info("skipping project {0}, no open date".format(p.name))

    log.info("Final stats :")
    log.info("{0}/{1} passed".format(passed, tested))
    if failed:
        log.error("Failed projects : {0}".format(", ".join(failed)))


def setupLog(args):
    mainlog = logging.getLogger('validationlogger')
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.FileHandler(args.logfile)#logs to file
    mfhs=logging.StreamHandler(sys.stderr)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mfhs.setFormatter(mft)
    mainlog.addHandler(mfh)
    mainlog.addHandler(mfhs)#logs to stderr
    return mainlog

if __name__ == "__main__":

    usage = "Usage:       python validation_core.py [options]"
    parser = argparse.ArgumentParser(description=usage)
    
    parser.add_argument("-d", "--days", dest="days", type=int, default=90,  
    help = "number of days to look back for projects open date")
    parser.add_argument("-c", "--conf", dest = "conf", default = os.path.join(
                      os.environ['HOME'],'opt/config/post_process.yaml'), help =
                      "Config file.  Default: ~/opt/config/post_process.yaml")
    parser.add_argument("-l", "--log", dest="logfile", default=os.path.join(
                      os.environ['HOME'],'LIMS2DB_validation.log'),  
    help = "number of days to look back for projects open date")

    args = parser.parse_args()
    validate_cores(args)
