import couchdb

from  genologics_sql.tables import *
from  genologics_sql.utils import *
from  genologics_sql.queries import get_last_modified_processes

import LIMS2DB.objectsDB.process_categories as pc_cg

from sqlalchemy import text

def create_lims_data_obj(session, pro):
    obj={}
    obj['step_id']=pro.luid

    #which container is used in this step ?
    query = "select distinct ct.* from container ct\
             inner join containerplacement cp on cp.containerid=ct.containerid \
             inner join processiotracker piot on piot.inputartifactid=cp.processartifactid \
             where piot.processid = {pid}::integer;".format(pid=pro.processid)

    cont=session.query(Container).from_statement(text(query)).first()
    obj['container_id']=cont.luid
    obj['container_name']=cont.name
    try:
        obj['container_running_notes']=cont.udfs[0].udfvalue
    except:
        continue

    if pc_cg.SEQUENCING.get(str(pro.typeid), '') == 'AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)':
        #NovaSeq flowcell have the individual stats as output artifact
        query = "select art.* from artifact art \
                 inner join outputmapping omap on omap.outputartifactid=art.artifactid \
                 inner join processiotracker piot on piot.trackerid=omap.trackerid \
                 where art.name LIKE 'Lane%' and piot.processid = {pid}::integer;".format(pid=pro.processid)
    else:
        #Which artifacts are updated in this step ?
        query = "select distinct art.* from artifact art\
                 inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                 where piot.processid = {pid}::integer;".format(pid=pro.processid)

    obj['run_summary']={}
    arts=session.query(Artifact).from_statement(text(query)).all()
    for art in arts:
        if pc_cg.SEQUENCING.get(str(pro.typeid), '') == 'AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)':
            lane = art.name.replace("Lane ", "")
        else:
            lane=art.containerplacement.api_string.split(":")[0]
        #lane is a string
        if lane.isalpha():
            lane=str(ord(lane)-64)
        obj['run_summary'][lane]=art.udf_dict
        obj['run_summary'][lane]['qc']=art.qc_flag


    return obj

def get_sequencing_steps(session, interval="24 hours"):
    #38, 46, 714 and 1454 are hiseq, miseq, hiseqX and novaseq sequencing
    return get_last_modified_processes(session, [38,714, 1454, 46], interval)

def upload_to_couch(couch, runid, lims_data):
    for dbname in ['flowcells', 'x_flowcells']:
        db=couch[dbname]
        view = db.view('info/id')
        doc=None
        for row in view[runid]:
            doc=db.get(row.value)

        if doc:
            doc['lims_data']=lims_data
            db.save(doc)
