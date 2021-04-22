
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.lims import Lims
from LIMS2DB.utils import setupLog
from genologics_sql.utils import *

def diff_project_objects(pj_id, couch, logfile, new=True):

    proj_db = couch['projects']
    samp_db = couch['samples']
    log = setupLog('diff - {}'.format(pj_id), logfile)
    lims = Lims(BASEURI, USERNAME, PASSWORD)

    view = proj_db.view('projects/lims_followed')

    try:
        old_project_couchid = view[pj_id].rows[0].value
    except (KeyError, IndexError):
        log.error("No such project {}".format(pj_id))


    old_project = proj_db.get(old_project_couchid)
    old_project.pop('_id', None)
    old_project.pop('_rev', None)
    old_project.pop('modification_time', None)
    old_project.pop('creation_time', None)

    if new:
        from LIMS2DB.classes import ProjectSQL
        session=get_session()
        host=get_configuration()['url']
        new_project=ProjectSQL(session, log, pj_id, host, couch)
    else:
        import LIMS2DB.objectsDB.objectsDB as DB
        new_project = DB.ProjectDB(lims, pj_id, samp_db, log)

    fediff=diff_objects(old_project, new_project.obj)

    return (fediff, old_project, new_project.obj)






def diff_objects(o1, o2, parent=''):
    diffs={}

    for key in o1:
        if key in o2:
            if isinstance(o1[key], dict):
                more_diffs=diff_objects(o1[key], o2[key], "{} {}".format(parent,key))
                diffs.update(more_diffs)

            else:
                if o1[key]!= o2[key]:
                    diffs["{} {}".format(parent, key)]=[o1[key], o2[key]]

        else:
            if o1[key]:
                diffs["key {} {}".format(parent, key)]=[o1[key], "missing"]

    for key in o2:
        if key not in o1 and o2[key]:
            diffs["key {} {}".format(parent, key)]=["missing", o2[key]]


    return diffs


if __name__ == "__main__":

    a={'a' : 1, 'b': 2, 'c':{'d':3, 'e':{'f':5}}}
    b={'a' : 1, 'b': 7, 'c':{'d':4, 'e':{'f':4}}}
    diffs=diff_objects(a,b)
    print(diffs)
