
import logging
import logging.handlers

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


def setupLog(name, args):
    mainlog = logging.getLogger(name)
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.FileHandler(args.logfile)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
    return mainlog
