from genologics_sql.tables import *
from sqlalchemy import text
from datetime import datetime
import LIMS2DB.objectsDB.process_categories as pc_cg


class Workset:

    def __init__(self, lims, crawler, log):
        self.log = log
        self.name = set()
        self.lims = lims
        self.obj={}
        #get the identifier
        outs = crawler.starting_proc.all_outputs()
        for out in outs:
            if out.type == "Analyte" and len(out.samples) == 1 :
                try:
                    self.name.add(out.location[0].name)
                except:
                    self.log.warn("no name found for workset {}".format(out.id))

        try:      
            self.obj['name'] = self.name.pop()
        except:
            self.log.error("No name found for current workset {}, might be an ongoing step.".format(crawler.starting_proc.id))
            raise NameError
        self.obj['technician']=crawler.starting_proc.technician.initials
        self.obj['id']=crawler.starting_proc.id
        self.obj['date_run']=crawler.starting_proc.date_run
        #only get the latest aggregate qc date
        latest_date=0
        for agr in crawler.libaggre:
            if agr.date_run > latest_date:
                latest_date=agr.date_run
        if not latest_date:
            latest_date=None
        self.obj['last_aggregate']=latest_date
        pjs = {}
        for p in crawler.projects:
            pjs[p.id] = {}
            pjs[p.id]['name'] = p.name
            try:
                pjs[p.id]['library'] = p.udf['Library construction method']
            except KeyError:
                pjs[p.id]['library'] = None
            try:
                pjs[p.id]['application'] = p.udf['Application']
            except KeyError:
                pjs[p.id]['application'] = None

            pjs[p.id]['samples'] = {}
            for sample in crawler.samples:
                if sample.project == p:
                    pjs[p.id]['samples'][sample.name] = {}
                    pjs[p.id]['samples'][sample.name]['library'] = {}
                    pjs[p.id]['samples'][sample.name]['sequencing'] = {}
                    try:
                        pjs[p.id]['samples'][sample.name]['customer_name'] = sample.udf['Customer Name']
                    except KeyError:
                        pjs[p.id]['samples'][sample.name]['customer_name'] = None


                    pjs[p.id]['samples'][sample.name]['rec_ctrl'] = {}
                    for i in crawler.starting_proc.all_inputs():
                        if sample in i.samples:
                            pjs[p.id]['samples'][sample.name]['rec_ctrl']['status'] = i.qc_flag
                       
                    for output in crawler.starting_proc.all_outputs():
                        if output.type == "Analyte" and sample in output.samples:
                            pjs[p.id]['samples'][sample.name]['location'] = output.location[1]

                    

                    for lib in sorted(crawler.libaggre, key=lambda l:l.date_run, reverse=True):
                        for inp in lib.all_inputs():
                            if sample in inp.samples :
                                onelib = {}
                                onelib['status'] = inp.qc_flag
                                onelib['art'] = inp.id
                                onelib['date'] = lib.date_run
                                onelib['name'] = lib.protocol_name
                                onelib['id'] = lib.id
                                if 'Concentration' in inp.udf and 'Conc. Units' in inp.udf :
                                    onelib['concentration']="{0} {1}".format(round(inp.udf['Concentration'], 2), inp.udf['Conc. Units'])
                                if 'Molar Conc. (nM)' in inp.udf :
                                    onelib['concentration']="{0} nM".format(round(inp.udf['Molar Conc. (nM)'], 2)) 
                                if 'Size (bp)' in inp.udf:
                                    onelib['size']=round(inp.udf['Size (bp)'],2)
                                if 'NeoPrep Machine QC' in inp.udf and onelib['status'] == 'UNKNOWN':
                                    onelib['status'] = inp.udf['NeoPrep Machine QC']

                                pjs[p.id]['samples'][sample.name]['library'][lib.id] = onelib
                                if 'library_status' not in  pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['library_status'] = inp.qc_flag


                    for seq in sorted(crawler.seq, key=lambda s:s.date_run, reverse=True):
                        for inp in seq.all_inputs():
                            if sample in inp.samples :
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id] = {}
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['status'] = inp.qc_flag
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['date'] = seq.date_run
                                if 'sequencing_status' not in  pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['sequencing_status'] = inp.qc_flag

        self.obj['projects'] = pjs
                    
class LimsCrawler:
    
    def __init__(self, lims,starting_proc=None, starting_inputs=None):
        self.lims = lims
        self.starting_proc = starting_proc
        self.samples = set()
        self.projects = set()
        self.finlibinitqc=set()
        self.initqc=set()
        self.initaggr=set()
        self.pooling=set()
        self.preprepstart = set()
        self.prepstart = set()
        self.prepend = set()
        self.libval = set()
        self.finliblibval = set()
        self.libaggre = set()
        self.dilstart = set()
        self.seq = set()
        self.demux = set()
        self.caliper = set()
        self.projsum = set()
        self.inputs = set()
        if starting_proc:
            for i in starting_proc.all_inputs():
                if i.type == "Analyte":
                    self.samples.update(i.samples)
                    self.inputs.add(i)
        if starting_inputs:
            for i in starting_inputs:
                if i.type == "Analyte":
                    self.samples.update(i.samples)
                    self.inputs.add(i)
        for sample in self.samples:
            if sample.project:
                self.projects.add(sample.project)

    def crawl(self,starting_step=None):
        nextsteps=set()
        if not starting_step:
            if not self.starting_proc:
                for i in self.inputs:
                    if i.type == "Analyte" and (self.samples.intersection(i.samples)):
                        nextsteps.update(self.lims.get_processes(inputartifactlimsid=i.id))
            else:
                starting_step=self.starting_proc
        if starting_step:
            for o in starting_step.all_outputs():
                if o.type == "Analyte" and (self.samples.intersection(o.samples)):
                    nextsteps.update(self.lims.get_processes(inputartifactlimsid=o.id))
        for step in nextsteps:
            if step.type.name in pc_cg.PREPREPSTART.values():
                self.preprepstart.add(step)
            elif step.type.name in pc_cg.PREPSTART.values():
                self.prepstart.add(step)
            elif step.type.name in pc_cg.PREPEND.values():
                self.prepend.add(step)
            elif step.type.name in pc_cg.LIBVAL.values():
                self.libval.add(step)
            elif step.type.name in pc_cg.AGRLIBVAL.values():
                self.libaggre.add(step)
            elif step.type.name in pc_cg.SEQUENCING.values():
                self.seq.add(step)
            elif step.type.name in pc_cg.DEMULTIPLEX.values():
                self.demux.add(step)
            elif step.type.name in pc_cg.INITALQCFINISHEDLIB.values():
                self.finlibinitqc.add(step)
            elif step.type.name in pc_cg.INITALQC.values():
                self.initqc.add(step)
            elif step.type.name in pc_cg.AGRINITQC.values():
                self.initaggr.add(step)
            elif step.type.name in pc_cg.POOLING.values():
                self.pooling.add(step)
            elif step.type.name in pc_cg.DILSTART.values():
                self.dilstart.add(step)
            elif step.type.name in pc_cg.SUMMARY.values():
                self.projsum.add(step)
            elif step.type.name in pc_cg.CALIPER.values():
                self.caliper.add(step)

            #if the step has analytes as outputs
            if filter(lambda x : x.type=="Analyte", step.all_outputs()):
                self.crawl(starting_step=step)

class Workset_SQL:
    def __init__(self, session, log, step):
        self.log = log
        self.start=step
        self.name = set()
        self.session = session
        self.obj={}
        self.build()

    def build(self):
        self.obj['id']=self.start.luid
        self.obj['last_aggregate'] = None 
        if self.start.daterun:
            self.obj["date_run"]=self.start.daterun.strftime("%Y-%m-%d")
        else:
            self.obj["date_run"]=None

        query="select distinct co.* from processiotracker pio \
                inner join outputmapping om on om.trackerid=pio.trackerid \
                inner join containerplacement cp on cp.processartifactid=om.outputartifactid \
                inner join container co on cp.containerid=co.containerid \
                where pio.processid = {0};".format(self.start.processid)
        self.container=self.session.query(Container).from_statement(text(query)).one()
        self.obj["name"]=self.container.name

        query="select rs.initials from principals pr \
                inner join researcher rs on rs.researcherid=pr.researcherid \
                where principalid=:pid;"
        self.obj['technician']=self.session.query(Researcher.initials).from_statement(text(query)).params(pid=self.start.ownerid).scalar()

        #main part
        self.obj['projects']={}
        query="select art.* from artifact art \
                inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                where piot.processid = {0}".format(self.start.processid)

        input_arts=self.session.query(Artifact).from_statement(text(query)).all()
        
        for inp in input_arts:
            sample=inp.samples[0]
            project=sample.project
            if not project:
                continue #control samples do not have projects
            if project.luid not in self.obj['projects']:
                self.obj['projects'][project.luid]={'application' : project.udf_dict.get('Application'), 
                                                    'name' : project.name, 
                                                    'library' : project.udf_dict.get('Library construction method'), 
                                                    'samples' : {}}
            if sample.name not in self.obj['projects'][project.luid]['samples']:
                self.obj['projects'][project.luid]['samples'][sample.name]={'customer_name' : sample.udf_dict.get('Customer Name'), 
                                                                            'sequencing_status' : 'UNKNOWN', 'library_status' : 'UNKNOWN', 
                                                                            'rec_ctrl' : {}, 'library' : {}, 'sequencing':{}}

            self.obj['projects'][project.luid]['samples'][sample.name]['rec_ctrl']['status']=inp.qc_flag

            query="select art.* from artifact art \
            inner join outputmapping om on om.outputartifactid=art.artifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            where piot.inputartifactid={inp_art} and art.artifacttypeid=2 and piot.processid={start_id};".format(inp_art=inp.artifactid, start_id=self.start.processid)

            out=self.session.query(Artifact).from_statement(text(query)).one()
            self.obj['projects'][project.luid]['samples'][sample.name]['location']=out.containerplacement.api_string
            
            query="select pc.* from process pc \
                    inner join processiotracker piot on piot.processid=pc.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                    where pc.typeid in ({agr_qc}) and aam.ancestorartifactid={out_art} order by daterun;".format(agr_qc=",".join(pc_cg.AGRLIBVAL.keys()), out_art=out.artifactid)

            aggregates=self.session.query(Process).from_statement(text(query)).all()

            for agr in aggregates:
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]={}
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['id']=agr.luid
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['name']=agr.protocolnameused
                if agr.daterun is not None:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['date']=agr.daterun.strftime("%Y-%m-%d")
                    if  not self.obj['last_aggregate'] or datetime.strptime(self.obj['last_aggregate'], '%Y-%m-%d') < agr.daterun:
                        self.obj['last_aggregate']=agr.daterun.strftime("%Y-%m-%d")
                else:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['date']=None


                query="select art.* from artifact art \
                        inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                        inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                        where piot.processid={processid} and aam.ancestorartifactid={ancestorid};".format(processid=agr.processid, ancestorid=out.artifactid)

                agr_inp=self.session.query(Artifact).from_statement(text(query)).one()
                if agr.typeid==806 and agr_inp.qc_flag=="UNKNOWN":
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['status']=agr_inp.udf_dict.get("NeoPrep Machine QC")
                    self.obj['projects'][project.luid]['samples'][sample.name]['library_status']=agr_inp.udf_dict.get("NeoPrep Machine QC")
                else:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['status']=agr_inp.qc_flag
                    self.obj['projects'][project.luid]['samples'][sample.name]['library_status']=agr_inp.qc_flag
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['art']=agr_inp.luid
                if 'Molar Conc. (nM)' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['concentration']="{0:.2f} nM".format(agr_inp.udf_dict['Molar Conc. (nM)'])
                elif 'Concentration' in agr_inp.udf_dict and 'Conc. Units' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['concentration']="{0:.2f} {1}".format(agr_inp.udf_dict['Concentration'], agr_inp.udf_dict['Conc. Units'])
                if 'Size (bp)' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['size']=round(agr_inp.udf_dict['Size (bp)'],2)
            
            query="select pc.* from process pc \
                    inner join processiotracker piot on piot.processid=pc.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                    where pc.typeid in ({seq}) and aam.ancestorartifactid={out_art} order by daterun;".format(seq=",".join(pc_cg.SEQUENCING.keys()), out_art=out.artifactid)

            sequencing=self.session.query(Process).from_statement(text(query)).all()
            for seq in sequencing:
                if seq.daterun is not None:
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid]={}
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid]['date']=seq.daterun.strftime("%Y-%m-%d")

                    query="select art.* from artifact art \
                            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                            inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                            where piot.processid={processid} and aam.ancestorartifactid={ancestorid};".format(processid=seq.processid, ancestorid=out.artifactid)

                    seq_inputs=self.session.query(Artifact).from_statement(text(query)).all()
                    seq_qc_flag="UNKNOWN"
                    for seq_inp in seq_inputs:
                        if seq_qc_flag != 'FAILED':#failed stops sequencing update
                            seq_qc_flag=seq_inp.qc_flag

                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid]['status']=seq_qc_flag
                    #updates every time until the latest one, because of the order by in fetching sequencing processes.
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing_status']=seq_qc_flag

