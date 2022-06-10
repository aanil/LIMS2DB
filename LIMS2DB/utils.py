
import logging
import logging.handlers
import traceback
import couchdb

#merges d2 in d1, keeps values from d1
def merge(d1, d2):
    """ Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param s2: Dictionary object
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key])
            elif d1[key] == d2[key]:
                pass # same leaf value
        else:
            d1[key] = d2[key]
    return d1


def setupLog(name, logfile):
    mainlog = logging.getLogger(name)
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
    return mainlog


def formatStack(stack):
    formatted_error=[]
    for trace in stack:
        formatted_error.append("File {f}: line {l} in {i}\n{e}".format(f=trace[0], l=trace[1], i=trace[2], e=trace[3]))

    return "\n".join(formatted_error)

def setupServer(conf):
    db_conf = conf['statusdb']
    url="https://{0}:{1}@{2}".format(db_conf['username'], db_conf['password'], db_conf['url'])
    return couchdb.Server(url)
