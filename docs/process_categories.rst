
What is a Process Category?
============================

In the project-statusdb context, lims processes are categorised into groups that define, or are used to define a certain type of status-db key in a project database. The categories are specified here. 


Adding a work flow.
==========================

If a work flow does not fit with the categories one might have to change the category definitions or ad new categories. This needs to be done in corperation with the developer of project_summary_uppload_LIMS.py. The cathegories are defined in process_categories.py within the objectsDB package.

SEQSTART
===============================


=== =======================================
ID  process Name
=== =======================================
26	Denature, Dilute and Load Sample (MiSeq) 4.0
710	Cluster Generation (HiSeq X) 1.0
23	Cluster Generation (Illumina SBS) 4.0
=== =======================================
    

LIBVALFINISHEDLIB
===============================


=== =======================================
ID  process Name
=== =======================================
24	Customer Gel QC
20	CaliperGX QC (DNA)
17	Bioanalyzer QC (Library Validation) 4.0
62	qPCR QC (Library Validation) 4.0
64	Quant-iT QC (Library Validation) 4.0
67	Qubit QC (Library Validation) 4.0
=== =======================================
    

PREPREPSTART
===============================


=== =======================================
ID  process Name
=== =======================================
74	Shear DNA (SS XT) 4.0
304	Ligate 3' adapters (TruSeq small RNA) 1.0
=== =======================================
    

INITALQCFINISHEDLIB
===============================


=== =======================================
ID  process Name
=== =======================================
24	Customer Gel QC
20	CaliperGX QC (DNA)
17	Bioanalyzer QC (Library Validation) 4.0
62	qPCR QC (Library Validation) 4.0
64	Quant-iT QC (Library Validation) 4.0
67	Qubit QC (Library Validation) 4.0
=== =======================================
    

AGRINITQC
===============================


=== =======================================
ID  process Name
=== =======================================
9	Aggregate QC (RNA) 4.0
7	Aggregate QC (DNA) 4.0
=== =======================================
    

POOLING
===============================


=== =======================================
ID  process Name
=== =======================================
308	Library Pooling (TruSeq Small RNA) 1.0
716	Library Pooling (HiSeq X) 1.0
255	Library Pooling (Finished Libraries) 4.0
58	Pooling For Multiplexed Sequencing (SS XT) 4.0
44	Library Pooling (TruSeq Amplicon) 4.0
45	Library Pooling (TruSeq Exome) 4.0
42	Library Pooling (Illumina SBS) 4.0
43	Library Pooling (MiSeq) 4.0
404	Pre-Pooling (Illumina SBS) 4.0
508	Applications Pre-Pooling
506	Pre-Pooling (MiSeq) 4.0
=== =======================================
    

CALIPER
===============================


=== =======================================
ID  process Name
=== =======================================
116	CaliperGX QC (RNA)
20	CaliperGX QC (DNA)
=== =======================================
    

WORKSET
===============================


=== =======================================
ID  process Name
=== =======================================
204	Setup Workset/Plate
=== =======================================
    

PREPEND
===============================


=== =======================================
ID  process Name
=== =======================================
157	Applications Finish Prep
311	Sample Placement (Size Selection)
456	Purification (ThruPlex)
406	End repair, size selection, A-tailing and adapter ligation (TruSeq PCR-free DNA) 4.0
109	CA Purification
111	Amplify Captured Libraries to Add Index Tags (SS XT) 4.0
805	NeoPrep Library Prep v1.0
=== =======================================
    

DILSTART
===============================


=== =======================================
ID  process Name
=== =======================================
39	Library Normalization (Illumina SBS) 4.0
715	Library Normalization (HiSeq X) 1.0
40	Library Normalization (MiSeq) 4.0
=== =======================================
    

INITALQC
===============================


=== =======================================
ID  process Name
=== =======================================
24	Customer Gel QC
63	Quant-iT QC (DNA) 4.0
20	CaliperGX QC (DNA)
65	Quant-iT QC (RNA) 4.0
66	Qubit QC (DNA) 4.0
16	Bioanalyzer QC (DNA) 4.0
68	Qubit QC (RNA) 4.0
18	Bioanalyzer QC (RNA) 4.0
504	Volume Measurement QC
116	CaliperGX QC (RNA)
=== =======================================
    

SUMMARY
===============================


=== =======================================
ID  process Name
=== =======================================
356	Project Summary 1.3
=== =======================================
    

LIBVAL
===============================


=== =======================================
ID  process Name
=== =======================================
20	CaliperGX QC (DNA)
17	Bioanalyzer QC (Library Validation) 4.0
62	qPCR QC (Library Validation) 4.0
64	Quant-iT QC (Library Validation) 4.0
67	Qubit QC (Library Validation) 4.0
504	Volume Measurement QC
=== =======================================
    

SEQUENCING
===============================


=== =======================================
ID  process Name
=== =======================================
46	MiSeq Run (MiSeq) 4.0
714	Illumina Sequencing (HiSeq X) 1.0
38	Illumina Sequencing (Illumina SBS) 4.0
=== =======================================
    

DEMULTIPLEX
===============================


=== =======================================
ID  process Name
=== =======================================
13	Bcl Conversion & Demultiplexing (Illumina SBS) 4.0
=== =======================================
    

PREPSTART
===============================


=== =======================================
ID  process Name
=== =======================================
407	Fragment DNA (ThruPlex)
10	Aliquot Libraries for Hybridization (SS XT)
117	Applications Generic Process
612	Fragmentation & cDNA synthesis (TruSeq RNA) 4.0
454	ThruPlex template preparation and synthesis
33	Fragment DNA (TruSeq DNA) 4.0
47	mRNA Purification, Fragmentation & cDNA synthesis (TruSeq RNA) 4.0
308	Library Pooling (TruSeq Small RNA) 1.0
405	RiboZero depletion
605	Tagmentation, Strand displacement and AMPure purification
=== =======================================
    

AGRLIBVAL
===============================


=== =======================================
ID  process Name
=== =======================================
8	Aggregate QC (Library Validation) 4.0
806	NeoPrep Library QC v1.0
=== =======================================
    

