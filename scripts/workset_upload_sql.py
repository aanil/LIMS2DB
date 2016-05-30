import argparse

import LIMS2DB.classes as lclasses
import LIMS2DB.parallel as lpar
import LIMS2DB.utils as lutils

from genologics_sql.tables import *
from genologics_sql.utils import *
from genologics_sql.queries import *
from sqlalchemy import text

def main(args):
    
    if args.ws:
        log = lutils.setupLog('worksetlogger', args.logfile)
        session=get_session()
        step=session.query(Process).filter_by(luid=args.ws).one()
        ws=lclasses.Workset_SQL(session, log,step)
        from pprint import pprint
        pprint(ws.obj)



if __name__ == '__main__':
    usage = "Usage:       python workset_upload_sql.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument("-d", "--days", dest="days", type=int, default=90,  
    help = "number of days to look back for worksets")

    parser.add_argument("-p", "--procs", dest="procs", type=int, default=8 ,  
    help = "number of processes to spawn")

    parser.add_argument("-w", "--workset", dest="ws", default=None,
    help = "tries to work on the given ws")

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    parser.add_argument("-q", "--queue", dest="queue", 
    help = "Internal testing parameter")

    parser.add_argument("-l", "--log", dest="logfile", 
    default=os.path.join(os.environ['HOME'],'workset_upload.log'), 
    help = "log file.  Default: ~/workset_upload.log")
    args = parser.parse_args()

    main(args)
