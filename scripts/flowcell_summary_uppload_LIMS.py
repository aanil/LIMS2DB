#!/usr/bin/env python

"""Script to load runinfo from the lims process: 'Illumina Sequencing (Illumina SBS) 4.0' 
into the flowcell database in statusdb.

Maya Brandi, Science for Life Laboratory, Stockholm, Sweden.
"""
import sys
import os
import codecs
from optparse import OptionParser
from pprint import pprint
from genologics.lims import *
from genologics.lims_utils import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from datetime import date
from statusdb.db.utils import *
from LIMS2DB.objectsDB.process_categories import *

lims = Lims(BASEURI, USERNAME, PASSWORD)
import logging

def get_run_qcs(fc, lanesobj):
    for art in fc.all_inputs():
        lane=art.location[1][0]
        if lane not in lanesobj:
            #should never happen if pm works
            lanesobj[lane]={}
        lanesobj[lane]['seq_qc_flag']=art.qc_flag
        dem=lims.get_processes(type=DEMULTIPLEX.values(), inputartifactlimsid=art.id)
        try:
            for outart in dem[0].all_outputs():
                if "FASTQ reads" not in outart.name:
                    continue
                else:
                    for outsample in outart.samples:
                        #this should be only one
                        lanesobj[lane][outsample.name]={}
                        lanesobj[lane][outsample.name]['dem_qc_flag']=outart.qc_flag

        except IndexError:
            #No demutiplexing found. this is fine.
            pass

def  main(flowcell, all_flowcells,days,conf,run_type):
    """If all_flowcells: all runs run less than a moth ago are uppdated"""
    today = date.today()
    couch = load_couch_server(conf)
    fc_db = couch['flowcells']
    xfc_db = couch['x_flowcells']
    process_dict = {'hiseq':'Illumina Sequencing (Illumina SBS) 4.0', 'miseq':'MiSeq Run (MiSeq) 4.0', 'hiseqx':'Illumina Sequencing (HiSeq X) 1.0'}    
    # Collect flowcell processes based upon how script is called
    if all_flowcells:
        flowcells = lims.get_processes(type = process_dict.values())
    elif flowcell:
        if not run_type:
            raise SystemExit("Option -f should always be called with option -t. kindly refer -h fo rmore info.")        
        fc_id = flowcell if run_type == 'miseq' else flowcell[1:]
        try:
            flowcells = [lims.get_processes(type = process_dict[run_type], udf = {'Flow Cell ID' : fc_id})[0]]
            days = float('inf') #no need of days check when a flowcell is specified
        except:
            raise SystemExit("Could not find any process for FC %s (type: %s)" % (flowcell, run_type))
    
    for fc in flowcells:
        try:
            closed = date(*map(int, fc.date_run.split('-')))
            delta = today-closed
            if not delta.days < days:
                continue
        except AttributeError:
            #Happens if fc has no date run, we should just not update and get to the next flowcell
            continue
        
        fc_is_hiseqx = False
        fc_udfs = dict(fc.udf.items())
        try:
            flowcell_name = fc_udfs['Flow Cell ID']
            if not '-' in flowcell_name:
                flowcell_name = "%s%s" % (fc_udfs['Flow Cell Position'],fc_udfs['Flow Cell ID'])
                if 'HiSeq X' in fc_udfs['SBS Kit Type']:
                    fc_is_hiseqx = True
        except KeyError:
            continue

        db_con = xfc_db if fc_is_hiseqx else fc_db  #this might be not neccesary in near future            
        key = find_flowcell_from_view(db_con, flowcell_name)
        if key:
            dbobj = db_con.get(key)
            logging.info('Fetched DB entry for FC %s with key %s' % (flowcell_name, key))
            dbobj["lims_data"] = {}
            dbobj["lims_data"]['step_id'] = fc.id
            dbobj["lims_data"]['container_id'] = fc.all_inputs()[0].location[0].id
            dbobj["lims_data"]['container_name'] = fc.all_inputs()[0].location[0].name
            dbobj["lims_data"]["run_summary"] = get_sequencing_info(fc) #located in genologics.lims_utils
#            get_run_qcs(fc, dbobj['lanes']) ## it is commented and not removed cause of uncertainity
            info = save_couchdb_obj(db_con, dbobj, add_time_log=False)
            logging.info('flowcell %s %s : _id = %s' % (flowcell_name, info, key))                

if __name__ == '__main__':
    usage = "Usage:       python flowcell_summary_upload_LIMS.py [options]"
    parser = OptionParser(usage=usage)

    parser.add_option("-f", "--flowcell", dest="flowcell_name", default=None, 
    help = "eg: AD1TAPACXX. Don't use with -a flag and -t should also be used along this.")
    
    parser.add_option("-t", "--type", dest="run_type", default=None, choices=['miseq', 'hiseq', 'hiseqx'],
    help = "Specify the type of run to look, should be specified when -f is given")

    parser.add_option("-a", "--all_flowcells", dest="all_flowcells", action="store_true", default=False, 
    help = "Uploads all Lims flowcells into couchDB. Don't use with -f flagg.")

    parser.add_option("-d", "--days", dest="days", default=30, type=int, 
    help="Runs older than DAYS days are not updated. Default is 30 days. Use with -a flagg")

    parser.add_option("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    (options, args) = parser.parse_args()
    logging.basicConfig(filename='fsul.log',level=logging.INFO)
    main(options.flowcell_name, options.all_flowcells, options.days, options.conf, options.run_type)
