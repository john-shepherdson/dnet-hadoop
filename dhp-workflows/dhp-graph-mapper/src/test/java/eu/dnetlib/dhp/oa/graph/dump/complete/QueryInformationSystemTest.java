
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static org.mockito.Mockito.lenient;

import java.util.*;
import java.util.function.Consumer;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
class QueryInformationSystemTest {

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		" and $x//context/param[./@name = 'status']/text() = 'all' " +
		"  return " +
		"$x//context";

	private static final String XQUERY_ENTITY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"where $x//context[./@type='community' or ./@type = 'ri'] and $x//context/param[./@name = 'status']/text() = 'all' return "
		+
		"concat(data($x//context/@id) , '@@', $x//context/param[./@name =\"name\"]/text(), '@@', " +
		"$x//context/param[./@name=\"description\"]/text(), '@@', $x//context/param[./@name = \"subject\"]/text(), '@@', "
		+
		"$x//context/param[./@name = \"zenodoCommunity\"]/text(), '@@', $x//context/@type)";

	List<String> communityMap = Arrays
		.asList(
			"clarin@@Common Language Resources and Technology Infrastructure@@CLARIN@@@@oac_clarin@@ri",
			"ee@@Sustainable Development Solutions Network - Greece@@The UN Sustainable Development Solutions Network (SDSN) has been operating since 2012 under the auspices of the UN Secretary-General. "
				+
				"SDSN mobilizes global scientific and technological expertise to promote practical solutions for sustainable development, including the implementation of the Sustainable Development Goals (SDGs) and the Paris Climate Agreement. The Greek hub of SDSN has been included in the SDSN network in 2017 and is co-hosted by ICRE8: International Center for Research on the Environment and the Economy and the Political Economy of Sustainable Development Lab.@@SDG13 - Climate action,SDG8 - Decent work and economic growth,SDG15 - "
				+
				"Life on land,SDG2 - Zero hunger,SDG17 - Partnerships for the ´goals,SDG10 - Reduced inequalities,SDG5 - Gender equality,SDG12 - Responsible     consumption and production,SDG14 - Life below water,SDG6 - Clean water and    sanitation,SDG11 - Sustainable cities and communities,SDG1 - No poverty,SDG3 -    Good health and well being,SDG7 - Affordable and clean energy,SDG4 - Quality    education,SDG9 - Industry innovation and infrastructure,SDG16 - Peace justice    and strong institutions@@oac_sdsn-greece@@community",
			"dh-ch@@Digital Humanities and Cultural Heritage@@This community gathers research results, data, scientific publications and projects related to the domain of Digital Humanities. This broad definition includes Humanities, Cultural Heritage, History, Archaeology and related fields.@@modern art,monuments,europeana data model,sites,field walking,frescoes,LIDO metadata schema,art history,excavation,Arts and Humanities General,cities,coins,temples,numismatics,lithics,roads,environmental archaeology,digital cultural heritage,archaeological reports,history,CRMba,churches,cultural heritage,archaeological stratigraphy,religious art,buidings,digital humanities,survey,archaeological sites,linguistic studies,bioarchaeology,architectural orders,palaeoanthropology,fine arts,europeana,CIDOC CRM,decorations,classic art,stratigraphy,digital archaeology,intangible cultural heritage,walls,humanities,chapels,CRMtex,Language and Literature,paintings,archaeology,fair data,mosaics,burials,architecture,medieval art,castles,CARARE metadata schema,statues,natural language processing,inscriptions,CRMsci,vaults,contemporary art,Arts and Humanities,CRMarchaeo,pottery,site,architectural,vessels@@oac_dh-ch@@community",
			"fam@@Fisheries and Aquaculture Management@@Conservation of marine resources for sustainable development. The Fisheries and Aquaculture community focus on resources (document, data, codes..) which have been produced in the framework of projects (H2020, FP7, ..) related to the domain of fisheries and aquaculture.@@Stock Assessment,pelagic,Acoustic,Fish farming,Fisheries,Fishermen,maximum sustainable yield,trawler,Fishing vessel,Fisherman,Fishing gear,mackerel,RFMO,Fish Aggregating Device,Bycatch,Fishery,common fisheries policy,Fishing fleet,Aquaculture@@fisheries@@community",
			"ni@@Neuroinformatics@@The neuroinformatics dashboard gathers research outputs from the 'neuroinformatics' community at large including the fields of: neuroscience, neuroinformatics, brain imaging databases and standards, brain imaging techniques, neuroimaging methods including statistics and machine learning. The dashboard covers a wide range of imaging methods including (but not limited to): MRI, TEP, EEG, MEG, and studies involving human participants as well as animal studies.@@brain mapping,brain imaging,electroencephalography,arterial spin labelling,brain fingerprinting,brain,neuroimaging,Multimodal Brain Image Analysis,fMRI,neuroinformatics,fetal brain,brain ultrasonic imaging,topographic brain mapping,diffusion tensor imaging,computerized knowledge assessment,connectome mapping,brain magnetic resonance imaging,brain abnormalities@@oac_ni@@community",
			"mes@@European Marine Science@@This community was initially defined to include a very broad range of topics, with the intention to generate a number of more focused and sustainable dashboards for research communities and initiatives. As outlined in the logo of this community, we intend to setup a community dashboard for EuroMarine (a consortium of 56 research and academic organisations) and monitoring dashboards for marine research initiatives, including infrastructures (e.g. EMBRC & EMSO), advisory boards (e.g. Marine Boards & ICES), and transnational funding bodies (e.g. JPI-Oceans and Tara Foundation).@@marine,ocean,fish,aqua,sea@@oac_mes@@community",
			"instruct@@Instruct-ERIC@@Instruct-ERIC is the European Research Infrastructure for Structural Biology@@@@oac_instruct@@community",
			"elixir-gr@@The Greek National Node of the ESFRI European RI ELIXIR@@ELIXIR-GR enhances the potential of the Greek bioinformatics community to offer open, easily accessible and state -of- the- art services to the Greek and the international academic community and other stakeholders, such as industry and the health sector. More importantly, by providing these services, the infrastructure facilitates discoveries in the field of the life-sciences, having strong spill over effects in promoting innovation in sectors such as discovery of new drug targets and development of novel therapeutic agents, development of innovative diagnostics, personalized medicine, and development of innovative biotechnological products and processes.@@@@oaa_elixir-gr@@ri",
			"aginfra@@Agricultural and Food Sciences@@The scope of this community is to provide access to publications, research data, projects and software that are related to agricultural and food sciences@@animal production and health,fisheries and aquaculture,food safety and human nutrition,information management,food technology,agri-food education and extension,natural resources and environment,food system,engineering technology and Research,agriculture,food safety risk assessment,food security,farming practices and systems,plant production and protection,agri-food economics and policy,Agri-food,food distribution,forestry@@oac_aginfra@@community",
			"dariah@@DARIAH EU@@The Digital Research Infrastructure for the Arts and Humanities (DARIAH) aims to enhance and support digitally-enabled research and teaching across the arts and humanities. It develops, maintains and operates an infrastructure in support of ICT-based research practices and sustains researchers in using them to build, analyse and interpret digital resources. DARIAH was established as a European Research Infrastructure Consortium (ERIC) in August 2014. Currently, DARIAH has 18 Members and several cooperating partners in eight non-member countries. Here you will find a growing collection of DARIAH-affiliated research outputs and other documents.    @@@@dariah@@ri",
			"epos@@European Plate Observing System@@EPOS, the European Plate Observing System, is a long-term plan to facilitate integrated use of data, data products, and facilities from distributed research infrastructures for solid Earth science in Europe.@@@@@@ri",
			"covid-19@@Corona Virus Disease@@This portal provides access to publications, research data, projects and software that may be relevant to the Corona Virus Disease (COVID-19). The OpenAIRE COVID-19 Gateway aggregates COVID-19 related records, links them and provides a single access point for discovery and navigation. We tag content from the OpenAIRE Research Graph (10,000+ data sources) and additional sources. All COVID-19 related research results are linked to people, organizations and projects, providing a contextualized navigation.@@COVID19,SARS-CoV,HCoV-19,mesh:C000657245,MERS-CoV,Síndrome Respiratorio Agudo Severo,mesh:COVID-19,COVID2019,COVID-19,SARS-CoV-2,2019 novel coronavirus,severe acute respiratory syndrome coronavirus 2,Orthocoronavirinae,Coronaviridae,mesh:D045169,coronavirus,SARS,coronaviruses,coronavirus disease-19,sars cov 2,Middle East Respiratory Syndrome,Severe acute respiratory syndrome coronavirus 2,Severe Acute Respiratory Syndrome,coronavirus disease 2019,2019-nCoV@@covid-19@@community");

	List<String> communityContext = Arrays
		.asList(
			"<context id=\"clarin\" label=\"CLARIN\" type=\"ri\">\n" +
				"    <param name=\"status\">all</param>\n" +
				"    <param name=\"description\">CLARIN</param>\n" +
				"    <param name=\"logourl\">https://www.clarin.eu/sites/default/files/clarin-frontpage-logo.jpg</param>\n"
				+
				"    <param name=\"name\">Common Language Resources and Technology Infrastructure</param>\n" +
				"    <param name=\"manager\">maria@clarin.eu,dieter@clarin.eu,f.m.g.dejong@uu.nl,paolo.manghi@isti.cnr.it</param>\n"
				+
				"    <param name=\"subject\"/>\n" +
				"    <param name=\"suggestedAcknowledgement\">(Part of) the work reported here was made possible by using the CLARIN infrastructure.</param>\n"
				+
				"    <param name=\"suggestedAcknowledgement\">The work reported here has received funding through &lt;CLARIN national consortium member, e.g. CLARIN.SI&gt;,  &lt;XYZ&gt; project, grant no. &lt;XYZ&gt;.</param>\n"
				+
				"    <param name=\"suggestedAcknowledgement\">The work reported here has received funding (through CLARIN ERIC) from the European Union’s Horizon 2020 research and innovation programme under grant agreement No &lt;0-9&gt; for project &lt;XYZ&gt;.\n"
				+
				"                    (E.g.  No 676529 for project CLARIN-PLUS.)</param>\n" +
				"    <param name=\"zenodoCommunity\">oac_clarin</param>\n" +
				"    <param name=\"creationdate\">2018-03-01T12:00:00</param>\n" +
				"    <category claim=\"true\" id=\"clarin::projects\" label=\"CLARIN Projects\">\n" +
				"        <concept claim=\"false\" id=\"clarin::projects::1\" label=\"CLARIN-PLUS\">\n" +
				"            <param name=\"projectfullname\">CLARIN-PLUS</param>\n" +
				"            <param name=\"suggestedAcknowledgement\"/>\n" +
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">676529</param>\n" +
				"            <param name=\"url\">http://www.clarin.eu</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"funding\">H2020-INFRADEV-1-2015-1</param>\n" +
				"            <param name=\"acronym\">CLARIN+</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"clarin::projects::2\" label=\"CLARIN\">\n" +
				"            <param name=\"projectfullname\">Common Language Resources and Technology Infrastructure</param>\n"
				+
				"            <param name=\"acronym\">CLARIN</param>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">212230</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"openaireId\">corda_______::ef782b2d85676aa3e5a907427feb18c4</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"clarin::contentproviders\" label=\"CLARIN Content providers\">" +
				"<!--<concept claim=\"true\" id=\"clarin::contentproviders::1\" label=\"Zotero\">\n" +
				"                        <param name=\"openaireId\">opendoar____::d96409bf894217686ba124d7356686c9</param>\n"
				+
				"                        <param name=\"name\">Public Knowledge Project EPrint Archive</param>\n" +
				"                        <param name=\"officialname\">Public Knowledge Project EPrint Archive</param>\n"
				+
				"                        <param name=\"enabled\">true</param>\n" +
				"                    </concept> -->\n" +
				"        <concept claim=\"false\" id=\"clarin::contentproviders::2\" label=\"\">\n" +
				"            <param name=\"name\">LINDAT/CLARIN repository</param>\n" +
				"            <param name=\"officialname\">LINDAT/CLARIN repository</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"true\" id=\"clarin::subcommunity\" label=\"CLARIN communities\">\n" +
				"        <concept claim=\"true\" id=\"clarin::subcommunity::1\" label=\"CLARIN-D\">\n" +
				"            <param name=\"fullname\">CLARIN-D</param>\n" +
				"            <param name=\"homepageUrl\">https://www.clarin-d.de/en/</param>\n" +
				"            <param name=\"suggestedAcknowledgement\"/>\n" +
				"            <param name=\"example\">http://www.lrec-conf.org/proceedings/lrec2018/pdf/504.pdf</param>\n"
				+
				"            <param name=\"nation\">Germany</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"clarin::zenodocommunities\" label=\"CLARIN Zenodo Communities\"/>\n"
				+
				"    <category claim=\"false\" id=\"clarin::organizations\" label=\"CLARIN Organizations\"/>\n" +
				"</context>",
			"<context id=\"dh-ch\" label=\"Digital Humanities and Cultural Heritage\" type=\"community\">\n" +
				"    <param name=\"status\">all</param>\n" +
				"    <param name=\"description\">This community gathers research results, data, scientific publications and projects related to the domain of Digital Humanities. This broad definition includes Humanities, Cultural Heritage, History, Archaeology and related fields.</param>\n"
				+
				"    <param name=\"logourl\">http://sanmamante.org/DH_CH_logo.png</param>\n" +
				"    <param name=\"name\">Digital Humanities and Cultural Heritage</param>\n" +
				"    <param name=\"manager\">ileniagalluccio87@gmail.com,achille.felicetti@gmail.com,paolo.manghi@isti.cnr.it,tim.evans@york.ac.uk</param>\n"
				+
				"    <param name=\"subject\">modern art,monuments,europeana data model,sites,field walking,frescoes,LIDO metadata schema,art history,excavation,Arts and Humanities General,cities,coins,temples,numismatics,lithics,roads,environmental archaeology,digital cultural heritage,archaeological reports,history,CRMba,churches,cultural heritage,archaeological stratigraphy,religious art,buidings,digital humanities,survey,archaeological sites,linguistic studies,bioarchaeology,architectural orders,palaeoanthropology,fine arts,europeana,CIDOC CRM,decorations,classic art,stratigraphy,digital archaeology,intangible cultural heritage,walls,humanities,chapels,CRMtex,Language and Literature,paintings,archaeology,fair data,mosaics,burials,architecture,medieval art,castles,CARARE metadata schema,statues,natural language processing,inscriptions,CRMsci,vaults,contemporary art,Arts and Humanities,CRMarchaeo,pottery,site,architectural,vessels</param>\n"
				+
				"    <param name=\"suggestedAcknowledgement\">The present work has been partially supported by the PARTHENOS project, funded by the European Commission (Grant Agreement No. 654119) under the HORIZON 2020 - INFRADEV-4-2014/2015 call</param>\n"
				+
				"    <param name=\"zenodoCommunity\">oac_dh-ch</param>\n" +
				"    <param name=\"creationdate\">2018-03-01T12:00:00</param>\n" +
				"    <category claim=\"false\" id=\"dh-ch::projects\" label=\"DH-CH Projects\">\n" +
				"        <concept claim=\"false\" id=\"dh-ch::projects::1\" label=\"Pooling Activities, Resources and Tools for Heritage E-research Networking, Optimization and Synergies\">\n"
				+
				"            <param name=\"projectfullname\">Pooling Activities, Resources and Tools for Heritage E-research Networking, Optimization and Synergies</param>\n"
				+
				"            <param name=\"suggestedAcknowledgement\">The present work has been partially supported by the PARTHENOS project, funded by the European Commission (Grant Agreement No. 654119) under the HORIZON 2020 - INFRADEV-4-2014/2015 call</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">654119</param>\n" +
				"            <param name=\"url\">http://www.parthenos-project.eu</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">PARTHENOS</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"dh-ch::contentproviders\" label=\"DH-CH Content providers\">\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::2\" label=\"The UK's largest collection of digital research data in the social sciences and humanities\">\n"
				+
				"            <param name=\"openaireId\">re3data_____::9ebe127e5f3a0bf401875690f3bb6b81</param>\n" +
				"            <param name=\"name\">The UK's largest collection of digital research data in the social sciences and humanities</param>\n"
				+
				"            <param name=\"officialname\">UK Data Archive</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::3\" label=\"Journal of Data Mining and Digital Humanities\">\n"
				+
				"            <param name=\"openaireId\">doajarticles::c6cd4b532e12868c1d760a8d7cda6815</param>\n" +
				"            <param name=\"name\">Journal of Data Mining and Digital Humanities</param>\n" +
				"            <param name=\"officialname\">Journal of Data Mining and Digital Humanities</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::6\" label=\"Frontiers in Digital Humanities\">\n"
				+
				"            <param name=\"openaireId\">doajarticles::a6de4499bb87bf3c01add0a9e2c9ed0b</param>\n" +
				"            <param name=\"name\">Frontiers in Digital Humanities</param>\n" +
				"            <param name=\"officialname\">Frontiers in Digital Humanities</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::7\" label=\"Il Capitale Culturale: Studies on the Value of Cultural Heritage\">\n"
				+
				"            <param name=\"openaireId\">doajarticles::6eb31d13b12bc06bbac06aef63cf33c9</param>\n" +
				"            <param name=\"name\">Il Capitale Culturale: Studies on the Value of Cultural Heritage</param>\n"
				+
				"            <param name=\"officialname\">Il Capitale Culturale: Studies on the Value of Cultural Heritage</param>\n"
				+
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::8\" label=\"Conservation Science in Cultural Heritage\">\n"
				+
				"            <param name=\"openaireId\">doajarticles::0da84e9dfdc8419576169e027baa8028</param>\n" +
				"            <param name=\"name\">Conservation Science in Cultural Heritage</param>\n" +
				"            <param name=\"officialname\">Conservation Science in Cultural Heritage</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::9\" label=\"Electronic Archiving System\">\n"
				+
				"            <param name=\"openaireId\">re3data_____::84e123776089ce3c7a33db98d9cd15a8</param>\n" +
				"            <param name=\"name\">Electronic Archiving System</param>\n" +
				"            <param name=\"officialname\">EASY</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::10\" label=\"DANS-KB Harvester\">\n" +
				"            <param name=\"openaireId\">openaire____::c5502a43e76feab55dd00cf50f519125</param>\n" +
				"            <param name=\"name\">DANS-KB Harvester</param>\n" +
				"            <param name=\"officialname\">Gemeenschappelijke Harvester DANS-KB</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::11\" label=\"ads\">\n" +
				"            <param name=\"openaireId\">re3data_____::a48f09c562b247a9919acfe195549b47</param>\n" +
				"            <param name=\"name\">ads</param>\n" +
				"            <param name=\"officialname\">Archaeology Data Service</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::12\" label=\"\">\n" +
				"            <param name=\"openaireId\">opendoar____::97275a23ca44226c9964043c8462be96</param>\n" +
				"            <param name=\"name\">KNAW Repository</param>\n" +
				"            <param name=\"officialname\">KNAW Repository</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"dh-ch::contentproviders::13\" label=\"Internet Archaeology\">\n"
				+
				"            <param name=\"openaireId\">doajarticles::2899208a99aa7d142646e0a80bfeef05</param>\n" +
				"            <param name=\"name\">Internet Archaeology</param>\n" +
				"            <param name=\"officialname\">Internet Archaeology</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"            <param name=\"selcriteria\"/>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"</context>\n",
			"<context id=\"ni\" label=\"Neuroinformatics\" type=\"community\">\n" +
				"    <param name=\"status\">all</param>\n" +
				"    <param name=\"description\">The neuroinformatics dashboard gathers research outputs from the 'neuroinformatics' community at large including the fields of: neuroscience, neuroinformatics, brain imaging databases and standards, brain imaging techniques, neuroimaging methods including statistics and machine learning. The dashboard covers a wide range of imaging methods including (but not limited to): MRI, TEP, EEG, MEG, and studies involving human participants as well as animal studies.</param>\n"
				+
				"    <param name=\"logourl\">https://docs.google.com/drawings/u/0/d/10e191xGoGf4uaRluMqbt_7cCj6LSCs2a29im4CmWjqU/export/png</param>\n"
				+
				"    <param name=\"name\">Neuroinformatics</param>\n" +
				"    <param name=\"manager\">sorina.pop@creatis.insa-lyon.fr,camille.maumet@inria.fr,christian.barillot@irisa.fr,xavier.rolland@irisa.fr,axel.bonnet@creatis.insa-lyon.fr,paolo.manghi@isti.cnr.it</param>\n"
				+
				"    <param name=\"subject\">brain mapping,brain imaging,electroencephalography,arterial spin labelling,brain fingerprinting,brain,neuroimaging,Multimodal Brain Image Analysis,fMRI,neuroinformatics,fetal brain,brain ultrasonic imaging,topographic brain mapping,diffusion tensor imaging,computerized knowledge assessment,connectome mapping,brain magnetic resonance imaging,brain abnormalities</param>\n"
				+
				"    <param name=\"suggestedAcknowledgement\"/>\n" +
				"    <param name=\"zenodoCommunity\">oac_ni</param>\n" +
				"    <param name=\"creationdate\">2018-03-01T12:00:00</param>\n" +
				"    <category claim=\"false\" id=\"ni::contentproviders\" label=\"NI Content providers\">\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::1\" label=\"OpenNeuro\">\n" +
				"            <param name=\"openaireId\">re3data_____::5b9bf9171d92df854cf3c520692e9122</param>\n" +
				"            <param name=\"name\">Formerly:OpenFMRI</param>\n" +
				"            <param name=\"officialname\">OpenNeuro</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::2\" label=\"RIO\">\n" +
				"            <param name=\"openaireId\">doajarticles::c7d3de67dc77af72f6747157441252ec</param>\n" +
				"            <param name=\"name\">Research Ideas and Outcomes</param>\n" +
				"            <param name=\"officialname\">Research Ideas and Outcomes</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::3\" label=\"NITRC\">\n" +
				"            <param name=\"openaireId\">re3data_____::8515794670370f49c1d176c399c714f5</param>\n" +
				"            <param name=\"name\">Neuroimaging Informatics Tools and Resources Clearinghouse</param>\n"
				+
				"            <param name=\"officialname\">NITRC</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::4\" label=\"FRONTIERSNI\">\n" +
				"            <param name=\"openaireId\">doajarticles::d640648c84b10d425f96f11c3de468f3</param>\n" +
				"            <param name=\"name\">Frontiers in Neuroinformatics</param>\n" +
				"            <param name=\"officialname\">Frontiers in Neuroinformatics</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::5\" label=\"NeuroImage: Clinical\">\n" +
				"            <param name=\"openaireId\">doajarticles::0c0e74daa5d95504eade9c81ebbd5b8a</param>\n" +
				"            <param name=\"name\">NeuroImage: Clinical</param>\n" +
				"            <param name=\"officialname\">NeuroImage: Clinical</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"ni::contentproviders::6\" label=\"NeuroVault\">\n" +
				"            <param name=\"openaireId\">rest________::fb1a3d4523c95e63496e3bc7ba36244b</param>\n" +
				"            <param name=\"name\">NeuroVault</param>\n" +
				"            <param name=\"officialname\">NeuroVault</param>\n" +
				"            <param name=\"enabled\">true</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"</context>\n",
			"<context id=\"instruct\" label=\"Instruct-ERIC\" type=\"ri\">\n" +
				"    <param name=\"status\">all</param>\n" +
				"    <param name=\"description\">Instruct-ERIC is the European Research Infrastructure for Structural Biology</param>\n"
				+
				"    <param name=\"logourl\">https://instruct-eric.eu/templates/instructeric/images/logos/instruct-eric-logo-noline.png</param>\n"
				+
				"    <param name=\"name\">Instruct-ERIC</param>\n" +
				"    <param name=\"manager\">claudia@instruct-eric.eu,carazo@cnb.csic.es,echrysina@eie.gr,susan@instruct-eric.eu,naomi@instruct-eric.eu,natalie@instruct-eric.eu,pmarie@igbmc.fr,darren.hart@ibs.fr,claudia@strubi.ox.ac.uk,paolo.manghi@isti.cnr.it</param>\n"
				+
				"    <param name=\"subject\"/>\n" +
				"    <param name=\"suggestedAcknowledgement\">The authors acknowledge the support and the use of resources of Instruct-ERIC.</param>\n"
				+
				"    <param name=\"suggestedAcknowledgement\">The authors acknowledge the support and the use of resources of Instruct (PID # or APPID #), a Landmark ESFRI project</param>\n"
				+
				"    <param name=\"zenodoCommunity\">oac_instruct</param>\n" +
				"    <param name=\"creationdate\">2018-03-01T12:00:00</param>\n" +
				"    <category claim=\"false\" id=\"instruct::projects\" label=\"Instruct-ERIC Projects\">\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::1\" label=\"Authentication and Authorisation For Research and Collaboration\">\n"
				+
				"            <param name=\"projectfullname\">Authentication and Authorisation For Research and Collaboration</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">730941</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-EINFRA-2016-1</param>\n" +
				"            <param name=\"acronym\">AARC2</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::2\" label=\"Building data bridges between biological and medical infrastructures in Europe\">\n"
				+
				"            <param name=\"projectfullname\">Building data bridges between biological and medical infrastructures in Europe</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">284209</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">FP7-INFRASTRUCTURES-2011-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">BioMedBridges</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::3\" label=\"Transnational access and enhancement of integrated Biological Structure determination at synchrotron X-ray radiation facilities\">\n"
				+
				"            <param name=\"projectfullname\">Transnational access and enhancement of integrated Biological Structure determination at synchrotron X-ray radiation facilities</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">283570</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">FP7-INFRASTRUCTURES-2011-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">BioStruct-X</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::4\" label=\"Coordinated Research Infrastructures Building Enduring Life-science services\">\n"
				+
				"            <param name=\"projectfullname\">Coordinated Research Infrastructures Building Enduring Life-science services</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">654248</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-INFRADEV-1-2014-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">CORBEL</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::5\" label=\"Infrastructure for NMR, EM and X-rays for translational research\">\n"
				+
				"            <param name=\"projectfullname\">Infrastructure for NMR, EM and X-rays for translational research</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">653706</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-INFRAIA-2014-2015</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">iNEXT</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::6\" label=\"Integrated Structural Biology Infrastructure\">\n"
				+
				"            <param name=\"projectfullname\">Integrated Structural Biology Infrastructure</param>\n" +
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">211252</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">FP7-INFRASTRUCTURES-2007-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">INSTRUCT</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::7\" label=\"Releasing the full potential of Instruct to expand and consolidate infrastructure services for integrated structural life science research\">\n"
				+
				"            <param name=\"projectfullname\">Releasing the full potential of Instruct to expand and consolidate infrastructure services for integrated structural life science research</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">731005</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-INFRADEV-2016-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">INSTRUCT-ULTRA</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::8\" label=\"Opening Synchrotron Light for Experimental Science and Applications in the Middle East\">\n"
				+
				"            <param name=\"projectfullname\">Opening Synchrotron Light for Experimental Science and Applications in the Middle East</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">730943</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-INFRASUPP-2016-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">OPEN SESAME</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::9\" label=\"Infrastructure for Protein Production Platforms\">\n"
				+
				"            <param name=\"projectfullname\">Infrastructure for Protein Production Platforms</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">227764</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">FP7-INFRASTRUCTURES-2008-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">PCUBE</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::10\" label=\"European Vaccine Research and Development Infrastructure\">\n"
				+
				"            <param name=\"projectfullname\">European Vaccine Research and Development Infrastructure</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">730964</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-INFRAIA-2016-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">TRAMSVAC2</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::11\" label=\"World-wide E-infrastructure for structural biology\">\n"
				+
				"            <param name=\"projectfullname\">World-wide E-infrastructure for structural biology</param>\n"
				+
				"            <param name=\"rule\"/>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">675858</param>\n" +
				"            <param name=\"url\"/>\n" +
				"            <param name=\"funding\">H2020-EINFRA-2015-1</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"acronym\">West-Life</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::projects::12\" label=\"RI-VIS\">\n" +
				"            <param name=\"projectfullname\">Expanding research infrastructure visibility to strengthen strategic partnerships</param>\n"
				+
				"            <param name=\"acronym\">RI-VIS</param>\n" +
				"            <param name=\"CD_PROJECT_NUMBER\">824063</param>\n" +
				"            <param name=\"funder\">EC</param>\n" +
				"            <param name=\"openaireId\">corda__h2020::af93b591b76991d8437993a8f6fc6538</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"instruct::contentproviders\" label=\"Instruct-ERIC Content providers\"/>\n"
				+
				"    <category claim=\"false\" id=\"instruct::zenodocommunities\" label=\"Instruct-ERIC Zenodo Communities\">\n"
				+
				"        <concept claim=\"false\" id=\"instruct::zenodocommunities::1\" label=\"Instruct\">\n" +
				"            <param name=\"zenodoid\">instruct</param>\n" +
				"            <param name=\"selcriteria\"/>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::zenodocommunities::2\" label=\"West-Life Virtual Research Environment for Structural Biology\">\n"
				+
				"            <param name=\"zenodoid\">west-life</param>\n" +
				"            <param name=\"selcriteria\"/>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"instruct::organizations\" label=\"Instruct-ERIC Organizations\">\n"
				+
				"        <concept claim=\"false\" id=\"instruct::organizations::1\" label=\"FRISBI\">\n" +
				"            <param name=\"name\">FRISBI</param>\n" +
				"            <param name=\"logourl\">aHR0cDovL2ZyaXNiaS5ldS9zdGF0aWMvaW1hZ2VzL2xvZ29zL2xvZ28tZnJpc2JpLnBuZw==</param>\n"
				+
				"            <param name=\"websiteurl\">aHR0cDovL2ZyaXNiaS5ldS8=</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::organizations::2\" label=\"RI-VIS\">\n" +
				"            <param name=\"name\">RI-VIS</param>\n" +
				"            <param name=\"logourl\">aHR0cHM6Ly9yaS12aXMuZXUvbmV0d29yay9yaXZpcy90ZW1wbGF0ZXMvcml2aXMvaW1hZ2VzL1JJLVZJU0xvZ29GaW5hbC0wNi5wbmc=</param>\n"
				+
				"            <param name=\"websiteurl\">aHR0cHM6Ly9yaS12aXMuZXU=</param>\n" +
				"        </concept>\n" +
				"        <concept claim=\"false\" id=\"instruct::organizations::3\" label=\"CIISB\">\n" +
				"            <param name=\"name\">CIISB</param>\n" +
				"            <param name=\"logourl\">aHR0cDovL2JpYy5jZWl0ZWMuY3ovZmlsZXMvMjkyLzEyNS5KUEc=</param>\n" +
				"            <param name=\"websiteurl\">aHR0cHM6Ly93d3cuY2lpc2Iub3Jn</param>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"</context>\n",
			"<context id=\"elixir-gr\" label=\"ELIXIR GR\" type=\"ri\">\n" +
				"    <param name=\"status\">all</param>\n" +
				"    <param name=\"description\">ELIXIR-GR enhances the potential of the Greek bioinformatics community to offer open, easily accessible and state -of- the- art services to the Greek and the international academic community and other stakeholders, such as industry and the health sector. More importantly, by providing these services, the infrastructure facilitates discoveries in the field of the life-sciences, having strong spill over effects in promoting innovation in sectors such as discovery of new drug targets and development of novel therapeutic agents, development of innovative diagnostics, personalized medicine, and development of innovative biotechnological products and processes.</param>\n"
				+
				"    <param name=\"logourl\">https://elixir-greece.org/sites/default/files/ELIXIR_GREECE_white_background.png</param>\n"
				+
				"    <param name=\"name\">The Greek National Node of the ESFRI European RI ELIXIR</param>\n" +
				"    <param name=\"manager\">vergoulis@imis.athena-innovation.gr,schatz@imis.athena-innovation.gr,paolo.manghi@isti.cnr.it</param>\n"
				+
				"    <param name=\"subject\"/>\n" +
				"    <param name=\"suggestedAcknowledgement\"/>\n" +
				"    <param name=\"zenodoCommunity\">oaa_elixir-gr</param>\n" +
				"    <param name=\"creationdate\">2018-03-01T12:00:00</param>\n" +
				"    <category claim=\"false\" id=\"elixir-gr::projects\" label=\"ELIXIR GR Projects\"/>\n" +
				"    <category claim=\"false\" id=\"elixir-gr::contentproviders\" label=\"Elixir-GR Content providers\">\n"
				+
				"        <concept claim=\"false\" id=\"elixir-gr::contentproviders::1\" label=\"bio.tools\">\n" +
				"            <param name=\"openaireId\">rest________::b8e502674c3c3499d5374e9b2ea6d8d5</param>\n" +
				"            <param name=\"name\">bio.tools</param>\n" +
				"            <param name=\"officialname\">bio.tools</param>\n" +
				"            <param name=\"enabled\">false</param>\n" +
				"            <param name=\"selcriteria\"/>\n" +
				"        </concept>\n" +
				"    </category>\n" +
				"    <category claim=\"false\" id=\"elixir-gr::zenodocommunities\" label=\"Elixir-GR Zenodo Communities\"/>\n"
				+
				"    <category claim=\"false\" id=\"elixir-gr::organizations\" label=\"Elixir-GR Organizations\">\n" +
				"        <concept claim=\"false\" id=\"elixir-gr::organizations::1\" label=\"ATHENA RC\">\n" +
				"            <param name=\"name\">ATHENA RC</param>\n" +
				"            <param name=\"logourl\">aHR0cHM6Ly9lbGl4aXItZ3JlZWNlLm9yZy9zaXRlcy9kZWZhdWx0L2ZpbGVzL3N0eWxlcy90aHVtYm5haWwvcHVibGljL3BhcnRuZXJfbG9nb3MvYXRoZW5hX2xvZ28uanBnP2l0b2s9VXdGWFNpZng=</param>\n"
				+
				"            <param name=\"websiteurl\">aHR0cHM6Ly93d3cuYXRoZW5hLWlubm92YXRpb24uZ3IvZW4=</param>\n" +
				"        </concept>\n" +
				"    </category><!--  <category claim=\"false\" id=\"elixir-gr::resultorganizations\" label=\"Elixir-GR Results through organizations\"/> -->\n"
				+
				"</context>");

	@Mock
	private ISLookUpService isLookUpService;

	private QueryInformationSystem queryInformationSystem;

	private Map<String, String> map;

	@BeforeEach
	public void setUp() throws ISLookUpException {
		lenient().when(isLookUpService.quickSearchProfile(XQUERY_ENTITY)).thenReturn(communityMap);
		lenient().when(isLookUpService.quickSearchProfile(XQUERY)).thenReturn(communityContext);
		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(isLookUpService);
	}

	@Test
	void testSizeEntity() throws ISLookUpException {

		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.getContextInformation(consumer);

		Assertions.assertEquals(12, cInfoList.size());
	}

	@Test
	void testSizeRelation() throws ISLookUpException {

		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.execContextRelationQuery();
		queryInformationSystem.getContextRelation(consumer, "contentproviders", ModelSupport.entityIdPrefix.get("datasource"));

		Assertions.assertEquals(5, cInfoList.size());
	}

	@Test
	void testContentRelation() throws ISLookUpException {

		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.execContextRelationQuery();
		queryInformationSystem.getContextRelation(consumer, "contentproviders", ModelSupport.entityIdPrefix.get("datasource"));

		cInfoList.forEach(contextInfo -> {
			switch (contextInfo.getId()) {
				case "elixir-gr":
					Assertions.assertEquals(1, contextInfo.getDatasourceList().size());
					Assertions
						.assertEquals(
							"10|rest________::b8e502674c3c3499d5374e9b2ea6d8d5",
							contextInfo.getDatasourceList().get(0));
					break;
				case "instruct":
					Assertions.assertEquals(0, contextInfo.getDatasourceList().size());
					break;
				case "ni":
					Assertions.assertEquals(6, contextInfo.getDatasourceList().size());
					Assertions
						.assertTrue(
							contextInfo
								.getDatasourceList()
								.contains("10|rest________::fb1a3d4523c95e63496e3bc7ba36244b"));
					break;
				case "dh-ch":
					Assertions.assertEquals(10, contextInfo.getDatasourceList().size());
					break;
				case "clarin":
					Assertions.assertEquals(0, contextInfo.getDatasourceList().size());
					break;
			}
		});
	}

	@Test
	void testContentEntity() throws ISLookUpException {

		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.getContextInformation(consumer);

		cInfoList.forEach(context -> {
			switch (context.getId()) {
				case "clarin":// clarin@@Common Language Resources and Technology Infrastructure@@CLARIN@@@@oac_clarin",
					Assertions
						.assertEquals("Common Language Resources and Technology Infrastructure", context.getName());
					Assertions.assertEquals("CLARIN", context.getDescription());
					Assertions
						.assertTrue(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals("oac_clarin", context.getZenodocommunity());
					Assertions.assertEquals("ri", context.getType());
					break;
				case "ee":
					Assertions.assertEquals("Sustainable Development Solutions Network - Greece", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(17, context.getSubject().size());
					Assertions.assertEquals("oac_sdsn-greece", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					break;
				case "dh-ch":
					Assertions.assertEquals("Digital Humanities and Cultural Heritage", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(67, context.getSubject().size());
					Assertions.assertEquals("oac_dh-ch", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					break;
				case "fam":
					Assertions.assertEquals("Fisheries and Aquaculture Management", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith("Conservation of marine resources for sustainable development"));
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(19, context.getSubject().size());
					Assertions.assertEquals("fisheries", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					break;
				case "ni":
					Assertions.assertEquals("Neuroinformatics", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith("The neuroinformatics dashboard gathers research outputs from the"));
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(18, context.getSubject().size());
					Assertions.assertEquals("oac_ni", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					Assertions.assertTrue(context.getSubject().contains("brain"));
					break;
				case "mes":
					Assertions.assertEquals("European Marine Science", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"This community was initially defined to include a very broad range of topics"));
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(5, context.getSubject().size());
					Assertions.assertEquals("oac_mes", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					Assertions.assertTrue(context.getSubject().contains("sea"));
					Assertions.assertTrue(context.getSubject().contains("fish"));
					Assertions.assertTrue(context.getSubject().contains("ocean"));
					Assertions.assertTrue(context.getSubject().contains("aqua"));
					Assertions.assertTrue(context.getSubject().contains("marine"));
					break;
				case "instruct":
					Assertions.assertEquals("Instruct-ERIC", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.equals(
									"Instruct-ERIC is the European Research Infrastructure for Structural Biology"));
					Assertions
						.assertTrue(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals("oac_instruct", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());

					break;
				case "elixir-gr":
					Assertions
						.assertEquals("The Greek National Node of the ESFRI European RI ELIXIR", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"ELIXIR-GR enhances the potential of the Greek bioinformatics community to offer open"));
					Assertions
						.assertTrue(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals("oaa_elixir-gr", context.getZenodocommunity());
					Assertions.assertEquals("ri", context.getType());

					break;
				case "aginfra":
					Assertions.assertEquals("Agricultural and Food Sciences", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"The scope of this community is to provide access to publications, research data, projects and software"));
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(18, context.getSubject().size());
					Assertions.assertEquals("oac_aginfra", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					Assertions.assertTrue(context.getSubject().contains("food distribution"));
					break;
				case "dariah":
					Assertions.assertEquals("DARIAH EU", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"The Digital Research Infrastructure for the Arts and Humanities (DARIAH) aims to enhance and support "));
					Assertions
						.assertTrue(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));

					Assertions.assertEquals("dariah", context.getZenodocommunity());
					Assertions.assertEquals("ri", context.getType());

					break;
				case "epos":
					Assertions.assertEquals("European Plate Observing System", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"EPOS, the European Plate Observing System, is a long-term plan to facilitate integrated use of "));
					Assertions
						.assertTrue(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));

					Assertions.assertEquals("", context.getZenodocommunity());
					Assertions.assertEquals("ri", context.getType());

					break;
				case "covid-19":
					Assertions.assertEquals("Corona Virus Disease", context.getName());
					Assertions.assertTrue(context.getDescription().length() > 0);
					Assertions
						.assertTrue(
							context
								.getDescription()
								.startsWith(
									"This portal provides access to publications, research data, projects and "));
					Assertions
						.assertFalse(
							Optional
								.ofNullable(context.getSubject())
								.map(value -> false)
								.orElse(true));
					Assertions.assertEquals(25, context.getSubject().size());
					Assertions.assertEquals("covid-19", context.getZenodocommunity());
					Assertions.assertEquals("community", context.getType());
					Assertions.assertTrue(context.getSubject().contains("coronavirus disease 2019"));
					break;

			}
		});

	}
}
