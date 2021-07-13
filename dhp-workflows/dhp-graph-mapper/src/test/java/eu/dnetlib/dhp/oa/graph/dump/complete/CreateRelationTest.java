
package eu.dnetlib.dhp.oa.graph.dump.complete;


import eu.dnetlib.dhp.schema.oaf.Project;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.utils.DHPUtils;

public class CreateRelationTest {

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
					"    <category claim=\"false\" id=\"ni::projects\" label=\"NI Content providers\"/>\n" +
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
				"    <category claim=\"false\" id=\"elixir-gr::projects\" label=\"ELIXIR GR Projects\">\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::12\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">BIO-INFORMATICS RESEARCH NETWORK COORDINATING CENTER (BIRN-CC)</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">1U24RR025736-01</param>\n" +
					"                        <param name=\"funder\">NIH</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::13\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">COLLABORATIVE RESEARCH: The Cognitive Neuroscience of Category Learning</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">0223843</param>\n" +
					"                        <param name=\"funder\">NSF</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::14\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">The Cognitive Atlas: Developing an Interdisciplinary Knowledge Base Through Socia</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">5R01MH082795-05</param>\n" +
					"                        <param name=\"funder\">NIH</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::15\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">Fragmented early life environmental and emotional / cognitive vulnerabilities</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">1P50MH096889-01A1</param>\n" +
					"                        <param name=\"funder\">NIH</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::16\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">Enhancement of the 1000 Functional Connectome Project</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">1R03MH096321-01A1</param>\n" +
					"                        <param name=\"funder\">TUBITAK</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::17\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">CRCNS Data Sharing: An open data repository for cognitive neuroscience: The OpenfMRI Project</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">1131441</param>\n" +
					"                        <param name=\"funder\">NSF</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::18\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">Enhancing Human Cortical Plasticity: Visual Psychophysics and fMRI</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">0121950</param>\n" +
					"                        <param name=\"funder\">NSF</param>\n" +
					"                    </concept>\n" +
					"                    <concept claim=\"false\" id=\"ni::projects::18\" label=\"\">\n" +
					"                        <param name=\"projectfullname\">Transforming statistical methodology for neuroimaging meta-analysis.</param>\n" +
					"                        <param name=\"acronym\"/>\n" +
					"                        <param name=\"CD_PROJECT_NUMBER\">100309</param>\n" +
					"                        <param name=\"funder\">WT</param>\n" +
					"                    </concept>\n" +
					"                </category>" +

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

	private QueryInformationSystem queryInformationSystem;

	private Map<String, String> map;

	@BeforeEach
	public void setUp() {

		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setContextRelationResult(communityContext);
	}

	@Test
	public void test1() {
		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);

		queryInformationSystem
			.getContextRelation(consumer, "contentproviders", ModelSupport.getIdPrefix(Datasource.class));

		cInfoList.forEach(c -> System.out.println(new Gson().toJson(c)));

		List<Relation> rList = new ArrayList<>();

		cInfoList.forEach(cInfo -> Process.getRelation(cInfo).forEach(rList::add));

		Assertions.assertEquals(34, rList.size());

		Assertions
			.assertTrue(
				rList
					.stream()
					.map(r -> r.getSource().getId())
					.collect(Collectors.toSet())
					.contains(
						String
							.format(
								"%s|%s::%s", Constants.CONTEXT_ID,
								Constants.CONTEXT_NS_PREFIX,
								DHPUtils.md5("dh-ch"))));

		Assertions
			.assertEquals(
				10,
				rList
					.stream()
					.filter(
						r -> r
							.getSource()
							.getId()
							.equals(
								String
									.format(
										"%s|%s::%s", Constants.CONTEXT_ID,
										Constants.CONTEXT_NS_PREFIX,
										DHPUtils.md5("dh-ch"))))
					.collect(Collectors.toList())
					.size());

		Assertions
			.assertEquals(
				10,
				rList
					.stream()
					.filter(
						r -> r
							.getTarget()
							.getId()
							.equals(
								String
									.format(
										"%s|%s::%s", Constants.CONTEXT_ID,
										Constants.CONTEXT_NS_PREFIX,
										DHPUtils.md5("dh-ch"))))
					.collect(Collectors.toList())
					.size());

		Set<String> tmp = rList
			.stream()
			.filter(
				r -> r
					.getSource()
					.getId()
					.equals(
						String
							.format(
								"%s|%s::%s", Constants.CONTEXT_ID,
								Constants.CONTEXT_NS_PREFIX,
								DHPUtils.md5("dh-ch"))))
			.map(r -> r.getTarget().getId())
			.collect(Collectors.toSet());

		Assertions
			.assertTrue(
				tmp.contains("10|re3data_____::9ebe127e5f3a0bf401875690f3bb6b81") &&
					tmp.contains("10|doajarticles::c6cd4b532e12868c1d760a8d7cda6815") &&
					tmp.contains("10|doajarticles::a6de4499bb87bf3c01add0a9e2c9ed0b") &&
					tmp.contains("10|doajarticles::6eb31d13b12bc06bbac06aef63cf33c9") &&
					tmp.contains("10|doajarticles::0da84e9dfdc8419576169e027baa8028") &&
					tmp.contains("10|re3data_____::84e123776089ce3c7a33db98d9cd15a8") &&
					tmp.contains("10|openaire____::c5502a43e76feab55dd00cf50f519125") &&
					tmp.contains("10|re3data_____::a48f09c562b247a9919acfe195549b47") &&
					tmp.contains("10|opendoar____::97275a23ca44226c9964043c8462be96") &&
					tmp.contains("10|doajarticles::2899208a99aa7d142646e0a80bfeef05"));

	}

	@Test
	public void test2() {
		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);

		queryInformationSystem
				.getContextRelation(consumer, "projects", ModelSupport.getIdPrefix(Project.class));

		cInfoList.forEach(c -> System.out.println(new Gson().toJson(c)));


		List<Relation> rList = new ArrayList<>();

		cInfoList.forEach(cInfo -> Process.getRelation(cInfo).forEach(rList::add));

		Assertions.assertEquals(44 , rList.size());

		Assertions
				.assertFalse(
						rList
								.stream()
								.map(r -> r.getSource().getId())
								.collect(Collectors.toSet())
								.contains(
										String
												.format(
														"%s|%s::%s", Constants.CONTEXT_ID,
														Constants.CONTEXT_NS_PREFIX,
														DHPUtils.md5("dh-ch"))));

		Assertions
				.assertEquals(
						2,
						rList
								.stream()
								.filter(
										r -> r
												.getSource()
												.getId()
												.equals(
														String
																.format(
																		"%s|%s::%s", Constants.CONTEXT_ID,
																		Constants.CONTEXT_NS_PREFIX,
																		DHPUtils.md5("clarin"))))
								.collect(Collectors.toList())
								.size());

		Assertions
				.assertEquals(
						2,
						rList
								.stream()
								.filter(
										r -> r
												.getTarget()
												.getId()
												.equals(
														String
																.format(
																		"%s|%s::%s", Constants.CONTEXT_ID,
																		Constants.CONTEXT_NS_PREFIX,
																		DHPUtils.md5("clarin"))))
								.collect(Collectors.toList())
								.size());

		Set<String> tmp = rList
				.stream()
				.filter(
						r -> r
								.getSource()
								.getId()
								.equals(
										String
												.format(
														"%s|%s::%s", Constants.CONTEXT_ID,
														Constants.CONTEXT_NS_PREFIX,
														DHPUtils.md5("clarin"))))
				.map(r -> r.getTarget().getId())
				.collect(Collectors.toSet());

		Assertions
				.assertTrue(
						tmp.contains("40|corda__h2020::b5a4eb56bf84bef2ebc193306b4d423f") &&
								tmp.contains("40|corda_______::ef782b2d85676aa3e5a907427feb18c4") );

		rList.forEach(rel -> {
			if (rel.getSource().getId().startsWith("40|")){
				String proj = rel.getSource().getId().substring(3);
				Assertions.assertTrue(proj.substring(0, proj.indexOf("::")).length() == 12);
			}
		});

	}
}
