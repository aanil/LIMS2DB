#!/usr/bin/env python

import argparse
from genologics.entities import Process
from genologics.lims import *
from genologics.lims_utils import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from statusdb.db.utils import *
from datetime import datetime, timedelta

import LIMS2DB.objectsDB.process_categories as pc 
import statusdb.db as sdb
import LIMS2DB.utils as lutils
import LIMS2DB.classes as lclasses
import LIMS2DB.parallel as lpar


def main(args):
    log = lutils.setupLog('worksetlogger', args.logfile)
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    #this will decide how far back we are looking
    if args.ws:
        wsp = Process(lims, id=args.ws)
        lc = lclasses.LimsCrawler(lims, wsp)
        lc.crawl()
        try:
            ws = lclasses.Workset(lims,lc, log)
        except NameError:
            log.error("no name found for this workset")
        #pprint(ws.obj)
        mycouch = sdb.Couch()
        mycouch.set_db("worksets")
        mycouch.connect()
        view = mycouch.db.view('worksets/name')
        #If there is already a workset with that name in the DB
        if len(view[ws.obj['name']].rows) == 1:
            remote_doc = view[ws.obj['name']].rows[0].value
            #remove id and rev for comparison
            doc_id = remote_doc.pop('_id')
            doc_rev = remote_doc.pop('_rev')
            if remote_doc != ws.obj:
                #if they are different, though they have the same name, upload the new one
                ws.obj=lutils.merge(ws.obj, remote_doc)
                ws.obj['_id'] = doc_id
                ws.obj['_rev'] = doc_rev
                mycouch.db[doc_id] = ws.obj 
                log.info("updating {0}".format(ws.obj['name']))
            else:
                log.info("not modifying {0}".format(ws.obj['name']))

        elif len(view[ws.obj['name']].rows) == 0:
            #it is a new doc, upload it
            mycouch.save(ws.obj) 
            log.info("saving {0}".format(ws.obj['name']))
        else:
            log.warn("more than one row with name {0} found".format(ws.obj['name']))
    else:
        try:
             from genologics_sql.queries import get_last_modified_processes, get_processes_in_history
             from genologics_sql.utils import get_session
             session=get_session()
             #Aggregate QC, Setup workset plate, or sequencing. 
             recent_processes=get_last_modified_processes(session,[8,204,38,714,46])
             #Setup workset plate is 204
             processes_to_update=[]
             for p in recent_processes:
                 processes_to_update.append(get_processes_in_history(session, p.processid, [204])

             wsts=[]
             for p in set(processes_to_update):
                wsts.append(Process(lims, id=p.luid))
                

        except ImportError:
            starting_date= datetime.today() - timedelta(args.days)
            str_date= starting_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            wsts = lims.get_processes(type=pc.WORKSET.values(),last_modified=str_date)

        log.info("the following processes will be updated : {0}".format(wsts))
        lpar.masterProcess(args, wsts, lims, log)
        #see parallel.py
    
    
    


if __name__ == '__main__':
    usage = "Usage:       python workset_upload.py [options]"
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
