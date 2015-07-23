
import LIMS2DB.objectsDB.process_categories as pc 

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
                                if 'Size (bp)' in inp.udf:
                                    onelib['size']=round(inp.udf['Size (bp)'],2)

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
            if step.type.name in pc.PREPREPSTART.values():
                self.preprepstart.add(step)
            elif step.type.name in pc.PREPSTART.values():
                self.prepstart.add(step)
            elif step.type.name in pc.PREPEND.values():
                self.prepend.add(step)
            elif step.type.name in pc.LIBVAL.values():
                self.libval.add(step)
            elif step.type.name in pc.AGRLIBVAL.values():
                self.libaggre.add(step)
            elif step.type.name in pc.SEQUENCING.values():
                self.seq.add(step)
            elif step.type.name in pc.DEMULTIPLEX.values():
                self.demux.add(step)
            elif step.type.name in pc.INITALQCFINISHEDLIB.values():
                self.finlibinitqc.add(step)
            elif step.type.name in pc.INITALQC.values():
                self.initqc.add(step)
            elif step.type.name in pc.AGRINITQC.values():
                self.initaggr.add(step)
            elif step.type.name in pc.POOLING.values():
                self.pooling.add(step)
            elif step.type.name in pc.DILSTART.values():
                self.dilstart.add(step)
            elif step.type.name in pc.SUMMARY.values():
                self.projsum.add(step)
            elif step.type.name in pc.CALIPER.values():
                self.caliper.add(step)

            #if the step has analytes as outputs
            if filter(lambda x : x.type=="Analyte", step.all_outputs()):
                self.crawl(starting_step=step)

