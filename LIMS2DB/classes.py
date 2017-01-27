from genologics_sql.tables import Artifact, Container, EscalationEvent, GlsFile, Process, Project, Researcher, ReagentType
from genologics_sql.queries import get_children_processes, get_processes_in_history
from LIMS2DB.diff import diff_objects
from sqlalchemy import text
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime

import LIMS2DB.objectsDB.process_categories as pc_cg
import re


class Workset:

    def __init__(self, lims, crawler, log):
        self.log = log
        self.name = set()
        self.lims = lims
        self.obj = {}
        # get the identifier
        outs = crawler.starting_proc.all_outputs()
        for out in outs:
            if out.type == "Analyte" and len(out.samples) == 1:
                try:
                    self.name.add(out.location[0].name)
                except:
                    self.log.warn("no name found for workset {}".format(out.id))

        try:
            self.obj['name'] = self.name.pop()
        except:
            self.log.error("No name found for current workset {}, might be an ongoing step.".format(crawler.starting_proc.id))
            raise NameError
        self.obj['technician'] = crawler.starting_proc.technician.initials
        self.obj['id'] = crawler.starting_proc.id
        self.obj['date_run'] = crawler.starting_proc.date_run
        # only get the latest aggregate qc date
        latest_date = 0
        for agr in crawler.libaggre:
            if agr.date_run > latest_date:
                latest_date = agr.date_run
        if not latest_date:
            latest_date = None
        self.obj['last_aggregate'] = latest_date
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

                    for lib in sorted(crawler.libaggre, key=lambda l: l.date_run, reverse=True):
                        for inp in lib.all_inputs():
                            if sample in inp.samples:
                                onelib = {}
                                onelib['status'] = inp.qc_flag
                                onelib['art'] = inp.id
                                onelib['date'] = lib.date_run
                                onelib['name'] = lib.protocol_name
                                onelib['id'] = lib.id
                                if 'Concentration' in inp.udf and 'Conc. Units' in inp.udf:
                                    onelib['concentration'] = "{0} {1}".format(round(inp.udf['Concentration'], 2), inp.udf['Conc. Units'])
                                if 'Molar Conc. (nM)' in inp.udf:
                                    onelib['concentration'] = "{0} nM".format(round(inp.udf['Molar Conc. (nM)'], 2))
                                if 'Size (bp)' in inp.udf:
                                    onelib['size'] = round(inp.udf['Size (bp)'], 2)
                                if 'NeoPrep Machine QC' in inp.udf and onelib['status'] == 'UNKNOWN':
                                    onelib['status'] = inp.udf['NeoPrep Machine QC']

                                pjs[p.id]['samples'][sample.name]['library'][lib.id] = onelib
                                if 'library_status' not in pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['library_status'] = inp.qc_flag

                    for seq in sorted(crawler.seq, key=lambda s: s.date_run, reverse=True):
                        for inp in seq.all_inputs():
                            if sample in inp.samples:
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id] = {}
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['status'] = inp.qc_flag
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['date'] = seq.date_run
                                if 'sequencing_status' not in pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['sequencing_status'] = inp.qc_flag

        self.obj['projects'] = pjs


class LimsCrawler:

    def __init__(self, lims, starting_proc=None, starting_inputs=None):
        self.lims = lims
        self.starting_proc = starting_proc
        self.samples = set()
        self.projects = set()
        self.finlibinitqc = set()
        self.initqc = set()
        self.initaggr = set()
        self.pooling = set()
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

    def crawl(self, starting_step=None):
        nextsteps = set()
        if not starting_step:
            if not self.starting_proc:
                for i in self.inputs:
                    if i.type == "Analyte" and (self.samples.intersection(i.samples)):
                        nextsteps.update(self.lims.get_processes(inputartifactlimsid=i.id))
            else:
                starting_step = self.starting_proc
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

            # if the step has analytes as outputs
            if filter(lambda x: x.type == "Analyte", step.all_outputs()):
                self.crawl(starting_step=step)


class Workset_SQL:

    def __init__(self, session, log, step):
        self.log = log
        self.start = step
        self.name = set()
        self.session = session
        self.obj = {}
        self.build()

    def extract_barcode(self, chain):
        bcp = re.compile("[ATCG\-]{4,}")
        if "NoIndex" in chain:
            return chain
        if '(' not in chain:
            barcode = chain
        else:
            pattern = re.compile("\(([A-Z\-]+)\)")
            matches = pattern.search(chain)
            if matches.group(1):
                barcode = matches.group(1)
        matches = bcp.match(barcode)
        if not matches:
            meta = self.session.query(ReagentType.meta_data).filter(ReagentType.name.like('%{}%'.format(barcode))).scalar()
            matches = bcp.search(meta)
            if matches:
                barcode = matches.group(0)
        return barcode

    def build(self):
        self.obj['id'] = self.start.luid
        self.obj['last_aggregate'] = None
        if self.start.daterun:
            self.obj["date_run"] = self.start.daterun.strftime("%Y-%m-%d")
        else:
            self.obj["date_run"] = None

        query = "select distinct co.* from processiotracker pio \
                inner join outputmapping om on om.trackerid=pio.trackerid \
                inner join containerplacement cp on cp.processartifactid=om.outputartifactid \
                inner join container co on cp.containerid=co.containerid \
                where pio.processid = {0};".format(self.start.processid)
        self.container = self.session.query(Container).from_statement(text(query)).one()
        self.obj["name"] = self.container.name

        query = "select rs.initials from principals pr \
                inner join researcher rs on rs.researcherid=pr.researcherid \
                where principalid=:pid;"
        self.obj['technician'] = self.session.query(Researcher.initials).from_statement(text(query)).params(pid=self.start.ownerid).scalar()

        # main part
        self.obj['projects'] = {}
        query = "select art.* from artifact art \
                inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                where piot.processid = {0}".format(self.start.processid)

        input_arts = self.session.query(Artifact).from_statement(text(query)).all()

        for inp in input_arts:
            sample = inp.samples[0]
            project = sample.project
            if not project:
                continue  # control samples do not have projects
            if project.luid not in self.obj['projects']:
                self.obj['projects'][project.luid] = {'application': project.udf_dict.get('Application'),
                                                      'name': project.name,
                                                      'library': project.udf_dict.get('Library construction method'),
                                                      'samples': {}}
                if project.closedate:
                    self.obj['projects'][project.luid]['close_date'] = project.closedate.strftime("%Y-%m-%d")
            if sample.name not in self.obj['projects'][project.luid]['samples']:
                self.obj['projects'][project.luid]['samples'][sample.name] = {'customer_name': sample.udf_dict.get('Customer Name'),
                                                                              'sequencing_status': 'UNKNOWN', 'library_status': 'UNKNOWN',
                                                                              'rec_ctrl': {}, 'library': {}, 'sequencing': {}}

            self.obj['projects'][project.luid]['samples'][sample.name]['rec_ctrl']['status'] = inp.qc_flag

            query = "select art.* from artifact art \
            inner join outputmapping om on om.outputartifactid=art.artifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            where piot.inputartifactid={inp_art} and art.artifacttypeid=2 and piot.processid={start_id};".format(inp_art=inp.artifactid, start_id=self.start.processid)

            out = self.session.query(Artifact).from_statement(text(query)).one()
            self.obj['projects'][project.luid]['samples'][sample.name]['location'] = out.containerplacement.api_string

            query = "select pc.* from process pc \
                    inner join processiotracker piot on piot.processid=pc.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                    where pc.typeid in ({agr_qc}) and aam.ancestorartifactid={out_art} order by daterun;".format(agr_qc=",".join(pc_cg.AGRLIBVAL.keys()), out_art=out.artifactid)

            aggregates = self.session.query(Process).from_statement(text(query)).all()

            for agr in aggregates:
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid] = {}
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['id'] = agr.luid
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['name'] = agr.protocolnameused
                if agr.daterun is not None:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['date'] = agr.daterun.strftime("%Y-%m-%d")
                    if not self.obj['last_aggregate'] or datetime.strptime(self.obj['last_aggregate'], '%Y-%m-%d') < agr.daterun:
                        self.obj['last_aggregate'] = agr.daterun.strftime("%Y-%m-%d")
                else:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['date'] = None

                query = "select art.* from artifact art \
                        inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                        inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                        where piot.processid={processid} and aam.ancestorartifactid={ancestorid};".format(processid=agr.processid, ancestorid=out.artifactid)

                agr_inp = self.session.query(Artifact).from_statement(text(query)).one()
                if agr.typeid == 806 and agr_inp.qc_flag == "UNKNOWN":
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['status'] = agr_inp.udf_dict.get("NeoPrep Machine QC")
                    self.obj['projects'][project.luid]['samples'][sample.name]['library_status'] = agr_inp.udf_dict.get("NeoPrep Machine QC")
                else:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['status'] = agr_inp.qc_flag
                    self.obj['projects'][project.luid]['samples'][sample.name]['library_status'] = agr_inp.qc_flag
                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['art'] = agr_inp.luid
                if 'Molar Conc. (nM)' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['concentration'] = "{0:.2f} nM".format(agr_inp.udf_dict['Molar Conc. (nM)'])
                elif 'Concentration' in agr_inp.udf_dict and 'Conc. Units' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['concentration'] = "{0:.2f} {1}".format(agr_inp.udf_dict['Concentration'], agr_inp.udf_dict['Conc. Units'])
                if 'Size (bp)' in agr_inp.udf_dict:
                    self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['size'] = round(agr_inp.udf_dict['Size (bp)'], 2)

                #Grabbing indexes
                # Get all artifacts for given sample
                query = "select art.* from artifact art \
                    inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
                    inner join sample sa on sa.processid=asm.processid \
                    where sa.processid = {sapid};".format(sapid=sample.processid)
                try:
                    artifacts = self.session.query(Artifact).from_statement(text(query)).all()
                    for art in artifacts:
                            if art.reagentlabels is not None and len(art.reagentlabels) == 1:
                                self.obj['projects'][project.luid]['samples'][sample.name]['library'][agr.luid]['index']=self.extract_barcode(art.reagentlabels[0].name)
                except AssertionError:
                    pass

            query = "select pc.* from process pc \
                    inner join processiotracker piot on piot.processid=pc.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                    where pc.typeid in ({seq}) and aam.ancestorartifactid={out_art} order by daterun;".format(seq=",".join(pc_cg.SEQUENCING.keys()), out_art=out.artifactid)

            sequencing = self.session.query(Process).from_statement(text(query)).all()
            for seq in sequencing:
                if seq.daterun is not None:
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid] = {}
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid]['date'] = seq.daterun.strftime("%Y-%m-%d")

                    query = "select art.* from artifact art \
                            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                            inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                            where piot.processid={processid} and aam.ancestorartifactid={ancestorid};".format(processid=seq.processid, ancestorid=out.artifactid)

                    seq_inputs = self.session.query(Artifact).from_statement(text(query)).all()
                    seq_qc_flag = "UNKNOWN"
                    for seq_inp in seq_inputs:
                        if seq_qc_flag != 'FAILED':  # failed stops sequencing update
                            seq_qc_flag = seq_inp.qc_flag

                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing'][seq.luid]['status'] = seq_qc_flag
                    # updates every time until the latest one, because of the order by in fetching sequencing processes.
                    self.obj['projects'][project.luid]['samples'][sample.name]['sequencing_status'] = seq_qc_flag


class ProjectSQL:

    def __init__(self, session, log, pid, host='genologics.scilifelab.se', couch=None):
        self.log = log
        self.pid = pid
        self.host = host
        self.name = set()
        self.session = session
        self.couch = couch
        self.obj = {}
        self.project = self.session.query(Project).filter(Project.luid == self.pid).one()
        self.build()

    def build(self):
        self.get_project_level()
        self.get_project_summary()
        self.get_escalations()
        self.get_samples()

    def save(self):
        doc = None
        db = self.couch['projects']
        view = db.view('project/project_id')
        for row in view[self.pid]:
            doc = db.get(row.id)
        if doc:
            my_id = doc.pop('_id', None)
            my_rev = doc.pop('_rev', None)
            my_mod = doc.pop('modification_time', None)
            my_crea = doc.pop('creation_time', None)
            diffs = diff_objects(doc, self.obj)
            if diffs:
                self.obj['_id'] = my_id
                self.obj['_rev'] = my_rev
                self.obj['creation_time'] = my_crea
                self.obj['modification_time'] = datetime.now().isoformat()
                self.log.info("Trying to save new doc for project {}".format(self.pid))
                db.save(self.obj)

        else:
            self.log.info("Trying to save new doc for project {}".format(self.pid))
            db.save(self.obj)

    def get_project_level(self):
        self.obj['entity_type'] = "project_summary"
        self.obj['source'] = "lims"
        self.obj['project_name'] = self.project.name
        self.obj['project_id'] = self.project.luid
        self.obj['application'] = self.project.udf_dict.get("Application")
        self.obj['contact'] = self.project.researcher.email
        if self.project.opendate:
            self.obj['open_date'] = self.project.opendate.strftime("%Y-%m-%d")
        if self.project.closedate:
            self.obj['close_date'] = self.project.closedate.strftime("%Y-%m-%d")
        if self.project.researcher.lab.udf_dict.get("Affiliation"):
            self.obj['affiliation'] = self.project.researcher.lab.udf_dict.get("Affiliation")
        if self.project.udf_dict.get("Uppnex ID"):
            self.obj['uppnex_id'] = self.project.udf_dict.get("Uppnex ID")
        if self.project.udf_dict.get("Reference genome"):
            self.obj['reference_genome'] = self.project.udf_dict.get("Reference genome")
        self.obj['details'] = self.make_normalized_dict(self.project.udf_dict)

    def get_project_summary(self):
        # get project summaries from project
        query = "select distinct pr.* from process pr \
            inner join processiotracker piot on piot.processid=pr.processid \
            inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.projectid = {pjid} and pr.typeid={tid} order by createddate desc limit 1;".format(pjid=self.project.projectid, tid=pc_cg.SUMMARY.keys()[0])
        try:
            pjs = self.session.query(Process).from_statement(text(query)).one()
            self.obj['project_summary'] = self.make_normalized_dict(pjs.udf_dict)
        except NoResultFound:
            self.log.info("No project summary found for project {}".format(self.project.projectid))

    def get_escalations(self):
        # get EscalationEvents from Project
        query = "select distinct esc.* from escalationevent esc \
                inner join processiotracker piot on piot.processid=esc.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where esc.reviewdate is NULL and sa.projectid = {pjid};".format(pjid=self.project.projectid)
        escalations = self.session.query(EscalationEvent).from_statement(text(query)).all()
        if escalations:
            self.obj['escalations'] = [str(esc.processid) for esc in escalations]

    def make_normalized_dict(self, d):
        ret = {}
        for kv in d.items():
            key = kv[0].lower().replace(" ", "_").replace('.', '')
            ret[key] = kv[1]
        return ret

    def get_samples(self):
        self.obj["no_of_samples"] = len(self.project.samples)
        self.obj['samples'] = {}
        for sample in self.project.samples:
            self.obj['samples'][sample.name] = {}
            self.obj['samples'][sample.name]['scilife_name'] = sample.name
            self.obj['samples'][sample.name]['customer_name'] = sample.udf_dict.get('Customer Name')
            self.obj['samples'][sample.name]['details'] = self.make_normalized_dict(sample.udf_dict)

            self.get_initial_qc(sample)
            self.get_library_preps(sample)

    def get_initial_qc(self, sample):
        # get all initial QC processes for sample
        query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun;".format(sapid=sample.processid, tid=','.join(pc_cg.INITALQC.keys() + pc_cg.INITALQCFINISHEDLIB.keys()))
        try:
            oldest_qc = self.session.query(Process).from_statement(text(query)).first()
            if not oldest_qc:
                return None
            self.obj['samples'][sample.name]['initial_qc'] = {}
            try:
                self.obj['samples'][sample.name]['initial_qc']['start_date'] = oldest_qc.daterun.strftime('%Y-%m-%d')
                self.obj['samples'][sample.name]['first_initial_qc_start_date'] = oldest_qc.daterun.strftime('%Y-%m-%d')
            except AttributeError:
                self.obj['samples'][sample.name]['initial_qc']['start_date'] = oldest_qc.createddate.strftime('%Y-%m-%d')
                self.obj['samples'][sample.name]['first_initial_qc_start_date'] = oldest_qc.createddate.strftime('%Y-%m-%d')

            try:
                if oldest_qc.daterun and datetime.strptime(self.obj['first_initial_qc'], '%Y-%m-%d') > oldest_qc.daterun:
                    self.obj['first_initial_qc'] = oldest_qc.daterun.strftime('%Y-%m-%d')
            except KeyError:
                try:
                    self.obj['first_initial_qc'] = oldest_qc.daterun.strftime('%Y-%m-%d')
                except AttributeError:
                    self.obj['first_initial_qc'] = oldest_qc.createddate.strftime('%Y-%m-%d')

            # get aggregate from init qc for sample
            query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun desc;".format(sapid=sample.processid, tid=','.join(pc_cg.AGRINITQC.keys()))
            try:
                youngest_aggregate = self.session.query(Process).from_statement(text(query)).first()
                try:
                    self.obj['samples'][sample.name]['initial_qc']['finish_date'] = youngest_aggregate.daterun.strftime('%Y-%m-%d')
                except AttributeError:
                    self.obj['samples'][sample.name]['initial_qc']['finish_date'] = youngest_aggregate.createddate.strftime('%Y-%m-%d')
                self.obj['samples'][sample.name]['initial_qc']['initials'] = youngest_aggregate.technician.researcher.initials
            except AttributeError:
                self.log.info("Didnt find an aggregate for Initial QC of sample {}".format(sample.name))
        except AttributeError:
            self.log.info("Did not find any initial QC for sample {}".format(sample.name))
        # Get initial artifact for given sample
        query = "select art.* from artifact art \
            inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sapid} and art.isoriginal=True".format(sapid=sample.processid)
        try:
            initial_artifact = self.session.query(Artifact).from_statement(text(query)).one()
            self.obj['samples'][sample.name]['initial_plate_id'] = initial_artifact.containerplacement.container.luid
            self.obj['samples'][sample.name]['well_location'] = initial_artifact.containerplacement.api_string
            self.obj['samples'][sample.name]['initial_qc']['initial_qc_status'] = initial_artifact.qc_flag
            self.obj['samples'][sample.name]['initial_qc'].update(self.make_normalized_dict(initial_artifact.udf_dict))
        except NoResultFound:
            self.log.info("did not find the initial artifact of sample {}".format(sample.name))
        # get GlsFile for output artifact of a Caliper process where its input is the initial artifact of a given sample
        query = "select gf.* from glsfile gf \
            inner join resultfile rf on rf.glsfileid=gf.fileid \
            inner join artifact art on rf.artifactid=art.artifactid \
            inner join outputmapping om on art.artifactid=om.outputartifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
            inner join process pr on piot.processid=pr.processid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.isoriginal=True and art.name like '%CaliperGX%{sname}' \
            order by pr.daterun desc;".format(sapid=sample.processid, tid=','.join(pc_cg.CALIPER.keys()), sname=sample.name)
        try:
            caliper_file = self.session.query(GlsFile).from_statement(text(query)).one()
            self.obj['samples'][sample.name]['initial_qc']['caliper_image'] = "sftp://{host}/home/glsftp/{uri}".format(host=self.host, uri=caliper_file.contenturi)
        except NoResultFound:
            self.log.info("Did not find an initial QC Caliper for sample {}".format(sample.name))

    def get_library_preps(self, sample):
        # first steps are either SetupWorksetPlate or Library Pooling Finished Libraries
        query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun;".format(sapid=sample.processid, tid=','.join(pc_cg.WORKSET.keys() + pc_cg.PREPSTARTFINLIB.keys() + ['117']))  # Applications Generic Process
        lp_starts = self.session.query(Process).from_statement(text(query)).all()
        prepid = 64
        for one_libprep in lp_starts:
            if 'library_prep' not in self.obj['samples'][sample.name]:
                self.obj['samples'][sample.name]['library_prep'] = {}

            # get all the output  artifacts of the libprep that match our sample
            query = "select art.* from artifact art \
            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
            inner join outputmapping om  on om.outputartifactid=art.artifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sapid} and piot.processid = {libid} and art.artifacttypeid = 2".format(sapid=sample.processid, libid=one_libprep.processid)
            lp_out_arts = self.session.query(Artifact).from_statement(text(query)).all()
            for one_libprep_art in lp_out_arts:
                prepid += 1
                prepname = chr(prepid)

                self.obj['samples'][sample.name]['library_prep'][prepname] = {}
                self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'] = {}
                self.obj['samples'][sample.name]['library_prep'][prepname]['workset_setup'] = one_libprep.luid

                if str(one_libprep.typeid) in pc_cg.PREPSTARTFINLIB:
                    self.obj['isFinishedLib'] = True

                # get a list of all libprep start steps
                try:
                    libp = get_children_processes(self.session, one_libprep.processid, pc_cg.PREPSTART, sample=sample.processid)
                    older = libp[0]
                    for l in libp:
                        if (not older.daterun and l.daterun) or (l.daterun and older.daterun > l.daterun):
                            older = l
                    try:
                        self.obj['samples'][sample.name]['library_prep'][prepname]['prep_start_date'] = older.daterun.strftime("%Y-%m-%d")
                        if "first_prep_start_date" not in self.obj['samples'][sample.name] or \
                                datetime.strptime(self.obj['samples'][sample.name]['first_prep_start_date'], "%Y-%m-%d") > older.daterun:
                            self.obj['samples'][sample.name]['first_prep_start_date'] = older.daterun.strftime("%Y-%m-%d")
                        self.obj['samples'][sample.name]['library_prep'][prepname]['prep_start_date'] = older.daterun.strftime("%Y-%m-%d")
                    except AttributeError:
                        # Missing date run
                        pass
                except IndexError:
                    self.log.info("No libstart found for sample {}".format(sample.name))
                    if one_libprep.typeid == 117:
                        if "first_prep_start_date" not in self.obj['samples'][sample.name] or \
                                datetime.strptime(self.obj['samples'][sample.name]['first_prep_start_date'], "%Y-%m-%d") > one_libprep.daterun:
                            self.obj['samples'][sample.name]['first_prep_start_date'] = one_libprep.daterun.strftime("%Y-%m-%d")
                        self.obj['samples'][sample.name]['library_prep'][prepname]['prep_start_date'] = one_libprep.daterun.strftime("%Y-%m-%d")
                pend = get_children_processes(self.session, one_libprep.processid, pc_cg.PREPEND, sample=sample.processid)
                try:
                    recent = pend[0]
                    for l in pend:
                        if (not recent.daterun and l.daterun) or (l.daterun and recent.daterun < l.daterun):
                            recent = l
                    self.obj['samples'][sample.name]['library_prep'][prepname]['prep_finished_date'] = recent.daterun.strftime("%Y-%m-%d")
                    self.obj['samples'][sample.name]['library_prep'][prepname]['prep_id'] = recent.luid
                except (IndexError, AttributeError):
                    self.log.info("no prepend for sample {} prep {}".format(sample.name, one_libprep.processid))

                try:
                    agrlibvals = get_children_processes(self.session, one_libprep.processid, pc_cg.AGRLIBVAL.keys(), sample.processid, 'daterun desc')
                    agrlibval = None
                    for agrlv in agrlibvals:
                        # for small rna (and maybe others), there is more than one agrlibval, and I should not get the latest one,
                        # but the latest one that ran at sample level, not a pool level.
                        # get input artifact of a given process that belongs to sample
                        query = "select art.* from artifact art \
                            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sapid} and piot.processid = {agrid}".format(sapid=sample.processid, agrid=agrlv.processid)
                        try:
                            inp_artifact = self.session.query(Artifact).from_statement(text(query)).first()

                            if len(inp_artifact.samples) > 1 and 'By user' not in self.obj['details']['library_construction_method']:
                                continue
                            else:
                                agrlibval = agrlv
                                break
                        except NoResultFound:
                            pass

                    # Get barcode for finlib
                    if 'By user' in self.obj['details']['library_construction_method']:
                        # Get initial artifact for given sample
                        query = "select art.* from artifact art \
                            inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sapid} and art.isoriginal=True".format(sapid=sample.processid)
                        try:
                            initial_artifact = self.session.query(Artifact).from_statement(text(query)).one()
                            self.obj['samples'][sample.name]['library_prep'][prepname]['reagent_label'] = initial_artifact.reagentlabels[0].name
                            self.obj['samples'][sample.name]['library_prep'][prepname]['barcode'] = self.extract_barcode(initial_artifact.reagentlabels[0].name)
                        except:
                            pass

                    # raises AttributeError on no aggregate
                    self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid] = {}
                    try:
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['finish_date'] = agrlibval.daterun.strftime("%Y-%m-%d")
                    except AttributeError:
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['finish_date'] = agrlibval.createddate.strftime("%Y-%m-%d")
                    self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['initials'] = agrlibval.technician.researcher.initials
                    # get input artifact of a given process that belongs to sample and descends from one_lp_art
                    query = "select art.* from artifact art \
                        inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                        inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                        inner join sample sa on sa.processid=asm.processid \
                        inner join artifact_ancestor_map aam on art.artifactid=aam.artifactid \
                        where sa.processid = {sapid} \
                        and piot.processid = {agrid} \
                        and aam.ancestorartifactid={lp_art}".format(sapid=sample.processid, agrid=agrlibval.processid, lp_art=one_libprep_art.artifactid)
                    try:
                        inp_artifact = self.session.query(Artifact).from_statement(text(query)).one()
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid].update(self.make_normalized_dict(inp_artifact.udf_dict))
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['prep_status'] = inp_artifact.qc_flag
                        self.obj['samples'][sample.name]['library_prep'][prepname]['prep_status'] = inp_artifact.qc_flag
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['well_location'] = inp_artifact.containerplacement.api_string
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['reagent_labels'] = [rg.name for rg in inp_artifact.reagentlabels]
                        if 'By user' not in self.obj['details']['library_construction_method']:
                            # if finlib, these are already computed
                            self.obj['samples'][sample.name]['library_prep'][prepname]['reagent_label'] = inp_artifact.reagentlabels[0].name
                            self.obj['samples'][sample.name]['library_prep'][prepname]['barcode'] = self.extract_barcode(inp_artifact.reagentlabels[0].name)
                        # get libval steps from the same input art
                        query = "select pr.* from process pr \
                            inner join processiotracker piot on piot.processid=pr.processid \
                            where pr.typeid in ({dem}) and piot.inputartifactid={iaid} \
                            order by pr.daterun;".format(dem=",".join(pc_cg.LIBVAL.keys()), iaid=inp_artifact.artifactid)
                        libvals = self.session.query(Process).from_statement(text(query)).all()
                        try:
                            self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['start_date'] = libvals[0].daterun.strftime("%Y-%m-%d")
                        except IndexError:
                            self.log.info("no library validation steps found for sample {} prep {}".format(sample.name, agrlibval.luid))
                            try:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['start_date'] = agrlibval.daterun.strftime("%Y-%m-%d")
                            except AttributeError:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['start_date'] = agrlibval.createddate.strftime("%Y-%m-%d")

                        # get GlsFile for output artifact of a Caliper process where its input is given
                        query = "select gf.* from glsfile gf \
                            inner join resultfile rf on rf.glsfileid=gf.fileid \
                            inner join artifact art on rf.artifactid=art.artifactid \
                            inner join outputmapping om on art.artifactid=om.outputartifactid \
                            inner join processiotracker piot on piot.trackerid=om.trackerid \
                            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
                            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                            inner join process pr on piot.processid=pr.processid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.artifactid={inpid} and art.name like '%CaliperGX%{sname}' \
                            order by pr.daterun desc;".format(sapid=sample.processid, inpid=inp_artifact.artifactid, tid=','.join(pc_cg.CALIPER.keys()), sname=sample.name)
                        try:
                            caliper_file = self.session.query(GlsFile).from_statement(text(query)).first()
                            self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['caliper_image'] = "sftp://{host}/home/glsftp/{uri}".format(host=self.host, uri=caliper_file.contenturi)
                        except AttributeError:
                            self.log.info("Did not find a libprep caliper image for sample {}".format(sample.name))
                    # handling neoprep
                        if "NeoPrep" in agrlibval.type.displayname:
                            self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['conc_units'] = "nM"
                            self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['concentration'] = inp_artifact.udf_dict['Normalized conc. (nM)']
                            # get output resultfile named like the sample of a Neoprep QC
                            query = "select art.* from artifact art \
                                inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                                inner join outputmapping om on art.artifactid=om.outputartifactid \
                                inner join processiotracker piot on piot.trackerid=om.trackerid \
                                inner join sample sa on sa.processid=asm.processid \
                                where art.artifacttypeid = 1 \
                                and art.name like '%{saname}%' \
                                and sa.processid = {sapid}\
                                and piot.processid = {agrid} \
                                and piot.inputartifactid = {inpid}".format(saname=sample.name, sapid=sample.processid, agrid=agrlibval.processid, inpid=inp_artifact.artifactid)
                            try:
                                out_art = self.session.query(Artifact).from_statement(text(query)).one()
                                self.obj['samples'][sample.name]['library_prep'][prepname]['prep_status'] = out_art.qc_flag
                                self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['prep_status'] = out_art.qc_flag
                                self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['reagent_labels'] = [rg.name for rg in out_art.reagentlabels]

                            except NoResultFound:
                                self.log.info("Did not find the output resultfile of the Neoprep step for sample {}".format(sample.name))
                    except NoResultFound:
                        pass
                    # cleaning up
                    if "size_(bp)" in self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]:
                        self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]["average_size_bp"] = \
                            self.obj['samples'][sample.name]['library_prep'][prepname]['library_validation'][agrlibval.luid]['size_(bp)']
                except AttributeError:
                    self.log.info("No aggregate for sample {} prep {}".format(sample.name, one_libprep.luid))
                # get output analyte of a given process that belongs to sample and has one_libprep_art as ancestor
                query = "select art.* from artifact art \
                    inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                    inner join outputmapping om on art.artifactid=om.outputartifactid \
                    inner join processiotracker piot on piot.trackerid=om.trackerid \
                    inner join sample sa on sa.processid=asm.processid \
                    inner join artifact_ancestor_map aam on art.artifactid=aam.artifactid \
                    where art.artifacttypeid = 2 \
                    and sa.processid = {sapid} \
                    and piot.processid = {agrid} \
                    and aam.ancestorartifactid = {libartid}".format(sapid=sample.processid, agrid=one_libprep.processid, libartid=one_libprep_art.artifactid)
                try:
                    out_artifact = self.session.query(Artifact).from_statement(text(query)).one()
                    self.obj['samples'][sample.name]['library_prep'][prepname]['workset_name'] = out_artifact.containerplacement.container.name
                    self.obj['samples'][sample.name]['library_prep'][prepname]['amount_taken_(ng)'] = out_artifact.udf_dict.get("Amount taken (ng)")
                    self.obj['samples'][sample.name]['library_prep'][prepname]['volume_(ul)'] = out_artifact.udf_dict.get("Total Volume (uL)")
                    # Legacy nextera special case
                    if not out_artifact.udf_dict.get("Amount taken (ng)"):
                        # get the output art of the tagmentation step
                        query = "select art.* from artifact art \
                                inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
                                inner join outputmapping om on om.outputartifactid=art.artifactid \
                                inner join processiotracker piot on piot.trackerid=om.trackerid \
                                inner join process pr on pr.processid=piot.processid \
                                inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                                where asm.processid={sid} and pr.typeid=605 and aam.ancestorartifactid={out_art}".format(sid=sample.processid, out_art=out_artifact.artifactid)
                        tag_out_art = self.session.query(Artifact).from_statement(text(query)).one()
                        self.obj['samples'][sample.name]['library_prep'][prepname]['amount_taken_(ng)'] = tag_out_art.udf_dict.get("Amount taken (ng)")

                except NoResultFound:
                    self.log.info("Did not find the output the Setup Workset Plate for sample {}".format(sample.name))
                # preprep
                query = "select pr.* from process pr \
                    inner join processiotracker piot on piot.processid=pr.processid \
                    inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                    inner join sample sa on sa.processid=asm.processid \
                    where sa.processid = {sapid} and pr.typeid in ({tid}) \
                    order by pr.daterun;".format(sapid=sample.processid, tid=','.join(pc_cg.PREPREPSTART.keys()))
                try:
                    preprep = self.session.query(Process).from_statement(text(query)).first()
                    self.obj['samples'][sample.name]['library_prep'][prepname]['pre_prep_start_date'] = preprep.daterun.strftime("%Y-%m-%d")
                except AttributeError:
                    self.log.info("Did not find a preprep for sample {}".format(sample.name))

                # get seqruns
                seqs = get_children_processes(self.session, one_libprep.processid, pc_cg.SEQUENCING.keys(), sample=sample.processid)
                for seq in seqs:
                    if 'sample_run_metrics' not in self.obj['samples'][sample.name]['library_prep'][prepname]:
                        self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'] = {}
                    seqstarts = get_processes_in_history(self.session, seq.processid, pc_cg.SEQSTART.keys(), sample=sample.processid)
                    dilstarts = get_processes_in_history(self.session, seq.processid, pc_cg.DILSTART.keys(), sample=sample.processid)
                    # get all the input artifacts of the seqrun that match our sample and our libprep
                    query = "select art.* from artifact art \
                    inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                    inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                    inner join sample sa on sa.processid=asm.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                    where sa.processid = {sapid} \
                    and piot.processid = {seqid} \
                    and aam.ancestorartifactid = {libpartid}".format(sapid=sample.processid, seqid=seq.processid, libpartid=one_libprep_art.artifactid)
                    inp_arts = self.session.query(Artifact).from_statement(text(query)).all()
                    for art in inp_arts:
                        if seq.typeid == 46:
                            # miseq
                            lane = art.containerplacement.api_string.split(":")[1]
                        else:
                            lane = art.containerplacement.api_string.split(":")[0]
                        self.obj['sequencing_finished'] = seq.udf_dict.get('Finish Date')
                        try:
                            run_id = seq.udf_dict["Run ID"]
                            date = run_id.split('_')[0]
                            fcid = run_id.split('_')[3]
                            seqrun_barcode = self.obj['samples'][sample.name]['library_prep'][prepname]['barcode']
                            samp_run_met_id = '_'.join([lane, date, fcid, seqrun_barcode])
                            self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id] = {}
                            self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_finish_date'] = seq.udf_dict.get('Finish Date')
                            self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['seq_qc_flag'] = art.qc_flag
                            try:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_start_date'] = seqstarts[0].daterun.strftime("%Y-%m-%d")
                            except AttributeError:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_start_date'] = seqstarts[0].createddate.strftime("%Y-%m-%d")
                            self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sample_run_metrics_id'] = self.find_couch_sampleid(samp_run_met_id)
                            try:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['dillution_and_pooling_start_date'] = dilstarts[0].daterun.strftime("%Y-%m-%d")
                            except AttributeError:
                                self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['dillution_and_pooling_start_date'] = dilstarts[0].createddate.strftime("%Y-%m-%d")
                            except IndexError:
                                self.log.info("no dilution found for sequencing {} of sample {}".format(seq.processid, sample.name))
                            # get the associated demultiplexing step
                            query = "select pr.* from process pr \
                                    inner join processiotracker piot on piot.processid=pr.processid \
                                    where pr.typeid={dem} and piot.inputartifactid={iaid};".format(dem=pc_cg.DEMULTIPLEX.keys()[0], iaid=art.artifactid)
                            try:
                                dem = self.session.query(Process).from_statement(text(query)).one()
                                try:
                                    self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_run_QC_finished'] = dem.daterun.strftime("%Y-%m-%d")
                                except AttributeError:
                                    self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_run_QC_finished'] = dem.createddate.strftime("%Y-%m-%d")

                                # get output resultfile named like the sample of a Demultiplex step
                                query = "select art.* from artifact art \
                                    inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                                    inner join outputmapping om on art.artifactid=om.outputartifactid \
                                    inner join processiotracker piot on piot.trackerid=om.trackerid \
                                    inner join sample sa on sa.processid=asm.processid \
                                    inner join artifact_ancestor_map aam on art.artifactid= aam.artifactid \
                                    where art.artifacttypeid = 1 \
                                    and art.name like '%{saname}%' \
                                    and sa.processid = {sapid} \
                                    and piot.processid = {dem}\
                                    and aam.ancestorartifactid = {artid};".format(saname=sample.name, sapid=sample.processid, dem=dem.processid, artid=art.artifactid)
                                out_arts = self.session.query(Artifact).from_statement(text(query)).all()
                                cumulated_flag = 'FAILED'
                                for art in out_arts:
                                    if art.qc_flag == 'PASSED':
                                        cumulated_flag = 'PASSED'

                                self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['dem_qc_flag'] = cumulated_flag

                            except NoResultFound:
                                try:
                                    self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_run_QC_finished'] = seq.daterun.strftime("%Y-%m-%d")
                                except AttributeError:
                                    self.obj['samples'][sample.name]['library_prep'][prepname]['sample_run_metrics'][samp_run_met_id]['sequencing_run_QC_finished'] = seq.createddate.strftime("%Y-%m-%d")

                            self.log.info("no demultiplexing found for sample {}, sequencing {}".format(sample.name, seq.processid))
                        except:
                            self.log.info("no run id for sequencing process {}".format(seq.luid))

    def extract_barcode(self, chain):
        bcp = re.compile("[ATCG\-]{4,}")
        if "NoIndex" in chain:
            return chain
        if '(' not in chain:
            barcode = chain
        else:
            pattern = re.compile("\(([A-Z\-]+)\)")
            matches = pattern.search(chain)
            if matches.group(1):
                barcode = matches.group(1)
        matches = bcp.match(barcode)
        if not matches:
            meta = self.session.query(ReagentType.meta_data).filter(ReagentType.name.like('%{}%'.format(barcode))).scalar()
            matches = bcp.search(meta)
            if matches:
                barcode = matches.group(0)
        return barcode

    def find_couch_sampleid(self, sample_run):
        db = self.couch['samples']
        view = db.view('names/name_to_id')
        for row in view[sample_run]:
            return row.id
