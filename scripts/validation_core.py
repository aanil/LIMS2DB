
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from scilifelab.db.statusDB_utils import load_couch_server

import LIMS2DB.objectsDB.objectsDB as newDB
import scilifelab.lims_utils.objectsDB as oldDB
import datetime
import argparse

def validate_cores(days):
    mylims = Lims(BASEURI, USERNAME, PASSWORD)
    pjs=mylims.get_projects()
    couch = load_couch_server('/Users/denismoreno/opt/config/post_process.yaml')
    proj_db = couch['projects']
    samp_db = couch['samples']
    tested=0
    passed=0
    failed=[]
    for p in pjs:
        if p.open_date:
            opDate=datetime.datetime.strptime(p.open_date, '%Y-%m-%d')
            now=datetime.datetime.now()

            if now-opDate < datetime.timedelta(days=days):
                tested+=1
                print "comparing project {0}".format(p.name)
                oldpj= oldDB.ProjectDB(mylims, p.id, samp_db)
                newpj= newDB.ProjectDB(mylims, p.id, samp_db)
                if oldpj.obj == newpj.obj:
                    print "passed"
                    passed+=1
                else:
                    print "PROJECT {0} FAILED COMPARISON".format(p.name)
                    failed.append(p.name)

            else:
                print "skipping project {0}, too old".format(p.name)
        else:
            print "skipping project {0}, no open date".format(p.name)

    print "Final stats :"
    print "{0}/{1} passed".format(passed, tested)
    if failed:
        print "Failed projects : {0}".format(", ".join(failed))


if __name__ == "__main__":

    usage = "Usage:       python validation_core.py [options]"
    parser = argparse.ArgumentParser(description=usage)
    
    parser.add_argument("-d", "--days", dest="days", type=int, default=90,  
    help = "number of days to look back for projects open date")

    args = parser.parse_args()
    validate_cores(args.days)
