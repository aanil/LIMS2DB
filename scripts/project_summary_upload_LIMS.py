#!/usr/bin/env python
"""Script to load project info from Lims into the project database in statusdb.

Maya Brandi, Science for Life Laboratory, Stockholm, Sweden.
"""
from __future__ import print_function
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.lims import *
from LIMS2DB.objectsDB.functions import *
from optparse import OptionParser
from statusdb.db.utils import *

from pprint import pprint

import codecs
import datetime
import LIMS2DB.objectsDB.objectsDB as DB
import logging
import logging.handlers
import multiprocessing as mp
import os
import Queue
import sys
import time

   
class PSUL():
    def __init__(self, proj, samp_db, proj_db, upload_data, man_name, output_f, log):
        self.proj = proj
        self.id = proj.id
        self.udfs = proj.udf
        self.name = proj.name
        self.open_date = proj.open_date
        self.close_date = proj.close_date
        self.samp_db = samp_db
        self.proj_db = proj_db
        self.upload_data = upload_data
        self.man_name = man_name
        self.output_f = output_f
        self.ordered_opened = self.get_ordered_opened()
        self.lims = Lims(BASEURI, USERNAME, PASSWORD)
        self.log=log

    def print_couchdb_obj_to_file(self, obj):
        if self.output_f is not None:
            with open(self.output_f, 'w') as f:
                print(obj, file = f)
        else:
            print(obj, file = sys.stdout)

    def get_ordered_opened(self):
        """Is project registered as opened or ordered?"""

        if self.open_date:
            return self.open_date
        elif 'Order received' in self.proj.udfs:
            return self.udfs['Order received'].isoformat()
        else:
            return datetime.date.today().strftime("%Y-%m-%d")

    def determine_update(self):
        """Determine wether to and how to update project"""
        opened_after_130630 = comp_dates('2013-06-30', self.ordered_opened)
        log_info = ''
        if not opened_after_130630 :
                start_update = False
                self.log.info('Project is not updated because: '
                    'It was opened or ordered before 2013-06-30 : '
                    '({ord_op})'.format(ord_op = self.ordered_opened))
        else:
            start_update = True

        return start_update

    def update_project(self):
        """Fetch project info and update project in the database."""
        self.log.info('Handling {proj}'.format(proj = self.name))
        project = DB.ProjectDB(self.lims, self.id, self.samp_db, self.log)

        key = find_proj_from_view(self.proj_db, self.name)
        project.obj['_id'] = find_or_make_key(key)
        if self.upload_data:
            info = save_couchdb_obj(self.proj_db, project.obj)
        else:
            info = self.print_couchdb_obj_to_file(project.obj)

        self.log.info("project {name} is handled and {info}: _id = {id}".format(
                           name=self.name, info=info, id=project.obj['_id']))

    def handle_project(self):
        if self.determine_update():
            self.update_project()

def main(options):
    man_name = options.project_name
    all_projects = options.all_projects
    conf = options.conf
    upload_data = options.upload
    output_f = options.output_f
    couch = load_couch_server(conf)
    proj_db = couch['projects']
    samp_db = couch['samples']
    mainlims = Lims(BASEURI, USERNAME, PASSWORD)
    mainlog = logging.getLogger('psullogger')
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(options.logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)

    if options.all_projects:
        if options.hours:
            delta=datetime.timedelta(hours=-options.hours)
            time_string=(datetime.datetime.now()-delta).strftime('%Y-%m-%dT%H:%M:%SCET')
            projects=mainlims.get_projects(last_modified=time_string)
        else:
            projects = mainlims.get_projects()
        masterProcess(options,projects, mainlims, mainlog)
    elif options.project_name:
        proj = mainlims.get_projects(name = options.project_name)
        if not proj:
            mainlog.warn('No project named {man_name} in Lims'.format(
                        man_name = options.project_name))
        else:
            P = PSUL(proj[0], samp_db, proj_db, options.upload, man_name, output_f, mainlog)
            P.handle_project()

def processPSUL(options, queue, logqueue):
    couch = load_couch_server(options.conf)
    proj_db = couch['projects']
    samp_db = couch['samples']
    mylims = Lims(BASEURI, USERNAME, PASSWORD)
    work=True
    procName=mp.current_process().name
    proclog=logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = QueueHandler(logqueue)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)

    while work:
        #grabs project from queue
        try:
            projname = queue.get(block=True, timeout=3)
        except Queue.Empty:
            work=False
            proclog.info("exiting gracefully")
            break
        else:
            #locks the project : cannot be updated more than once.
            lockfile=os.path.join(options.lockdir, projname)
            if not os.path.exists(lockfile):
                try:
                    open(lockfile,'w').close()
                except:
                    proclog.error("cannot create lockfile {}".format(lockfile))
                try:
                    proj=mylims.get_projects(name=projname)[0]
                    P = PSUL(proj, samp_db, proj_db, options.upload, options.project_name, options.output_f, proclog)
                    P.handle_project()
                except :
                    error=sys.exc_info()
                    proclog.error("{0}:{1}\n{2}".format(error[0], error[1], error[2]))
                try:
                    os.remove(lockfile)
                except:
                    proclog.error("cannot remove lockfile {}".format(lockfile))


            #signals to queue job is done
            queue.task_done()

def masterProcess(options,projectList, mainlims, logger):
    projectsQueue=mp.JoinableQueue()
    logQueue=mp.Queue()
    childs=[]
    #Initial step : order projects by sample number:
    logger.info("ordering the project list")
    orderedprojectlist=sorted(projectList, key=lambda x: (mainlims.get_sample_number(projectname=x.name)), reverse=True)
    logger.info("done ordering the project list")
    #spawn a pool of processes, and pass them queue instance 
    for i in range(options.processes):
        p = mp.Process(target=processPSUL, args=(options,projectsQueue, logQueue))
        p.start()
        childs.append(p)
    #populate queue with data   
    for proj in orderedprojectlist:
        projectsQueue.put(proj.name)

    #wait on the queue until everything has been processed     
    notDone=True
    while notDone:
        try:
            log=logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not stillRunning(childs):
                notDone=False
                break

def stillRunning(processList):
    ret=False
    for p in processList:
        if p.is_alive():
            ret=True

    return ret

class QueueHandler(logging.Handler):
    """
    This handler sends events to a queue. Typically, it would be used together
    with a multiprocessing Queue to centralise logging to file in one process
    (in a multi-process application), so as to avoid file write contention
    between processes.

    This code is new in Python 3.2, but this class can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def enqueue(self, record):
        """
        Enqueue a record.

        The base implementation uses put_nowait. You may want to override
        this method if you want to use blocking, timeouts or custom queue
        implementations.
        """
        self.queue.put_nowait(record)

    def prepare(self, record):
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            self.enqueue(self.prepare(record))
        except Exception:
            self.handleError(record)
                  
if __name__ == '__main__':
    usage = "Usage:       python project_summary_upload_LIMS.py [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--project", dest = "project_name", default = None,
                      help = "eg: M.Uhlen_13_01. Dont use with -a flagg.")
    parser.add_option("-a", "--all_projects", dest = "all_projects", action = 
                      "store_true", default = False, help = ("Upload all Lims ",
                      "projects into couchDB. Don't use with -f flagg."))
    parser.add_option("-c", "--conf", dest = "conf", default = os.path.join(
                      os.environ['HOME'],'opt/config/post_process.yaml'), help =
                      "Config file.  Default: ~/opt/config/post_process.yaml")
    parser.add_option("--no_upload", dest = "upload", default = True, action = 
                      "store_false", help = ("Use this tag if project objects ",
                      "should not be uploaded, but printed to output_f, or to ",
                      "stdout"))
    parser.add_option("--output_f", dest = "output_f", help = ("Output file",
                      " that will be used only if --no_upload tag is used"), default=None)
    parser.add_option("-m", "--multiprocs", type='int', dest = "processes", default = 4,
                      help = "How many processes will be spawned. Will only work with -a")
    parser.add_option("-l", "--logfile", dest = "logfile", help = ("log file",
                      " that will be used. default is $HOME/lims2db_projects.log "), default=os.path.expanduser("~/lims2db_projects.log"))
    parser.add_option("--lockdir", dest = "lockdir", help = ("directory handling the lock files",
                      " to avoid multiple updating of one project. default is $HOME/psul_locks "), default=os.path.expanduser("~/psul_locks"))
    parser.add_option("-j", "--hours", dest = "hours",type='int', help = ("only handle projects modified in the last X hours"), default=None)

    (options, args) = parser.parse_args()
    main(options)

