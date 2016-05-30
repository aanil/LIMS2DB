import argparse

import LIMS2DB.classes as lclasses
import LIMS2DB.parallel as lpar
import LIMS2DB.utils as lutils

from genologics_sql.tables import *
from genologics_sql.utils import *
from genologics_sql.queries import *
from sqlalchemy import text

def main(args):
    
    log = lutils.setupLog('worksetlogger', args.logfile)
    session=get_session()
    if args.ws:
        step=session.query(Process).filter_by(luid=args.ws).one()
        ws=lclasses.Workset_SQL(session, log, step)
        with open(args.conf) as conf_file:
            conf=yaml.load(conf_file)
        couch=lutils.setupServer(conf)
        db=couch["worksets"]
        doc={}
        for row in db.view('worksets/lims_id')[ws.obj['id']]:
            doc=db.get(row.id)
        
        if doc:
            final_doc=lutils.merge(ws.obj, doc)

        db.save(final_doc)



    elif args.recent:
         recent_processes=get_last_modified_processes(session,[8,204,38,714,46], args.interval)
         processes_to_update=[]
         for p in recent_processes:
             if p.typeid==204:
                 processes_to_update.append(p)
             else:
                 processes_to_update.extend(get_processes_in_history(session, p.processid, [204]))
         
         log.info("the following processes will be updated : {0}".format(processes_to_update))
         lpar.masterProcessSQL(args, processes_to_update, log)



        



if __name__ == '__main__':
    usage = "Usage:       python workset_upload_sql.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument("-p", "--procs", dest="procs", type=int, default=8 ,  
    help = "number of processes to spawn")

    parser.add_argument("-a", "--all", dest="recent", action='store_true', default=False,
    help = "tries to work on the recent worksets")

    parser.add_argument("-i", "--interval", dest="interval", default="2 hours",
    help = "interval to look at to grab worksets")

    parser.add_argument("-w", "--workset", dest="ws", default=None,
    help = "tries to work on the given ws")

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")


    parser.add_argument("-l", "--log", dest="logfile", 
    default=os.path.join(os.environ['HOME'],'workset_upload.log'), 
    help = "log file.  Default: ~/workset_upload.log")
    args = parser.parse_args()

    main(args)
