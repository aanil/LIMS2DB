import couchdb

from  genologics_sql.tables import *
from  genologics_sql.utils import *
from  genologics_sql.queries import get_last_modified_processes

from sqlalchemy import text

def create_lims_data_obj(session, pro):
    obj={}
    obj['step_id']=pro.luid

    query= "select distinct ct.* from container ct\
            inner join containerplacement cp on cp.containerid=ct.containerid \
            inner join processiotracker piot on piot.inputartifactid=cp.processartifactid \
            where piot.processid = {pid}::integer;".format(pid=pro.processid)
    
    cont=session.query(Container).from_statement(text(query)).first()
    obj['container_id']=cont.luid
    obj['container_name']=cont.name


    query= "select distinct art.* from artifact art\
            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
            where piot.processid = {pid}::integer;".format(pid=pro.processid)

    obj['run_summary']={}
    arts=session.query(Artifact).from_statement(text(query)).all()
    for art in arts:
        #lane is a string
        lane=art.containerplacement.api_string.split(":")[0]
        obj['run_summary'][lane]=art.udf_dict
        obj['run_summary'][lane]['qc']=art.qc_flag
        

    return obj

def get_sequencing_steps(session, interval="24 hours"):
    #38, 46 and 714 are hiseq, miseq and hiseqX sequencing
    return get_last_modified_processes(session, [38,714,46], interval)

def upload_to_couch(couch, runid, lims_data):
    for dbname in ['flowcells', 'x_flowcells']:
        db=couch[dbname]
        view = db.view('info/id')
        docs=None
        for row in view[runid]:
            doc=db.get(row.value)

        if doc:
            doc['lims_data']=lims_data
            db.save(doc)


    
   



if __name__=="__main__":
    session=get_session()
    pro=session.query(Process).filter(text("processid=158216")).one()
    data=create_lims_data_obj(pro)

