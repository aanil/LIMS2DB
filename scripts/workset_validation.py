
from genologics.lims import *
from genologics_sql.utils import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics_sql.tables import Process
from pprint import pprint

import genologics.entities as gent
import LIMS2DB.utils as lutils
import LIMS2DB.classes as lclasses


def main(ws_id):
    log = lutils.setupLog('worksetlogger', "out.log")
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    wsp = gent.Process(lims, id=ws_id)
    lc = lclasses.LimsCrawler(lims, wsp)
    lc.crawl()
    ws1 = lclasses.Workset(lims,lc, log)
    session=get_session()
    step=session.query(Process).filter_by(luid=ws_id).one()
    ws2=lclasses.Workset_SQL(session, log,step)
    diffs=my_comp(ws1.obj, ws2.obj)
    if diffs:
        print "\n".join(diffs)
        #print "##########################"
        #pprint(ws1.obj)
        #print "##########################"
        #pprint(ws2.obj)
    else:
        print "no diff found"


def my_comp(d1, d2, path='root'):
    diffs=[]

    for d in d1:
        if d in d2:
            if isinstance(d1[d], dict):
                newpath="{}/{}".format(path,d)
                diffs.extend(my_comp(d1[d], d2[d], newpath))
                
            else:
                if d1[d] != d2[d]:
                    diffs.append("Values '{}' differ at depth {} : '{}' vs '{}'".format(d, path, d1[d], d2[d]))

        else:
            diffs.append("key '{}' missing in right dict at depth {}".format(d, path))


    return diffs


if __name__ == "__main__":
    print '24-188975'
    main('24-188975')
    print '24-185529'
    main('24-185529')
    print '24-181946'
    main('24-181946')
    print '24-179366'
    main('24-179366')
    print '24-190011'
    main('24-190011')
    print '24-188979'
    main('24-188979')
