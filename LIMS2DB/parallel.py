
import logging
import logging.handlers
import LIMS2DB.classes as lclasses
import LIMS2DB.utils as lutils
import multiprocessing as mp
import statusdb.db as sdb
import Queue

from genologics.entities import Process
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.lims import *

def processWSUL(options, queue, logqueue):
    mycouch = sdb.Couch()
    mycouch.set_db("worksets")
    mycouch.connect()
    view = mycouch.db.view('worksets/name')
    mylims = Lims(BASEURI, USERNAME, PASSWORD)
    work=True
    procName = mp.current_process().name
    proclog = logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = QueueHandler(logqueue)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)

    while work:
        #grabs project from queue
        try:
            ws_id = queue.get(block=True, timeout=3)
        except Queue.Empty:
            work = False
            proclog.info("exiting gracefully")
            break
        else:
            wsp = Process(mylims, id=ws_id)
            lc = lclasses.LimsCrawler(mylims, wsp)
            try:
                ws = lclasses.Workset(mylims,lc, proclog)
            except NameError:
                continue

            #If there is already a workset with that name in the DB
            if len(view[ws.obj['name']].rows) == 1:
                remote_doc=view[ws.obj['name']].rows[0].value
                #remove id and rev for comparison
                doc_id = remote_doc.pop('_id')
                doc_rev = remote_doc.pop('_rev')
                if remote_doc != ws.obj:
                    #if they are different, though they have the same name, upload the new one
                    ws.obj=lutils.merge(ws.obj, remote_doc)
                    ws.obj['_id'] = doc_id
                    ws.obj['_rev'] = doc_rev
                    mycouch.db[doc_id] = ws.obj 
                    proclog.info("updating {0}".format(ws.obj['name']))
            elif len(view[ws.obj['name']].rows) == 0:
                #it is a new doc, upload it
                mycouch.save(ws.obj) 
                proclog.info("saving {0}".format(ws.obj['name']))
            else:
                proclog.warn("more than one row with name {0} found".format(ws.obj['name']))
            #signals to queue job is done
            queue.task_done()

def masterProcess(options,wslist, mainlims, logger):
    worksetQueue = mp.JoinableQueue()
    logQueue = mp.Queue()
    childs = []
    procs_nb = 1;
    #Initial step : order worksets by date:
    logger.info("ordering the workset list")
    orderedwslist = sorted(wslist, key=lambda x:x.date_run)
    logger.info("done ordering the workset list")
    if len(wslist) < options.procs:
        procs_nb = len(wslist)
    else:
        procs_nb = options.procs

    #spawn a pool of processes, and pass them queue instance 
    for i in range(procs_nb):
        p = mp.Process(target=processWSUL, args=(options,worksetQueue, logQueue))
        p.start()
        childs.append(p)
    #populate queue with data   
    for ws in orderedwslist:
        worksetQueue.put(ws.id)

    #wait on the queue until everything has been processed     
    notDone=True
    while notDone:
        try:
            log = logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not stillRunning(childs):
                notDone = False
                break

def stillRunning(processList):
    ret = False
    for p in processList:
        if p.is_alive():
            ret = True

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
                  


