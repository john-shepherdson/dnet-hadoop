
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static org.mockito.Mockito.lenient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.gson.Gson;

import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchCommunity;
import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchInitiative;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class CreateEntityTest {

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

	@Mock
	private ISLookUpService isLookUpService;

	private QueryInformationSystem queryInformationSystem;

	private static String workingDir;

	@BeforeEach
	public void setUp() throws ISLookUpException {
		lenient().when(isLookUpService.quickSearchProfile(XQUERY_ENTITY)).thenReturn(communityMap);
		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(isLookUpService);
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.complete.CreateEntityTest.class.getSimpleName())
			.toString();
	}

	@Test
	public void test1() throws ISLookUpException, IOException {
		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.getContextInformation(consumer);

		List<ResearchInitiative> riList = new ArrayList<>();
		cInfoList.forEach(cInfo -> riList.add(Process.getEntity(cInfo)));

		Assertions.assertEquals(12, riList.size());

		riList.stream().forEach(c -> {
			switch (c.getOriginalId()) {
				case "mes":
					Assertions
						.assertTrue(c.getType().equals(eu.dnetlib.dhp.oa.graph.dump.Constants.RESEARCH_COMMUNITY));
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().size() == 5);
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().contains("marine"));
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().contains("ocean"));
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().contains("fish"));
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().contains("aqua"));
					Assertions.assertTrue(((ResearchCommunity) c).getSubject().contains("sea"));
					Assertions
						.assertTrue(
							c
								.getId()
								.equals(
									String
										.format(
											"%s|%s::%s", Constants.CONTEXT_ID, Constants.CONTEXT_NS_PREFIX,
											DHPUtils.md5(c.getOriginalId()))));
					Assertions.assertTrue(c.getZenodo_community().equals("https://zenodo.org/communities/oac_mes"));
					Assertions.assertTrue("mes".equals(c.getOriginalId()));
					break;
				case "clarin":
					Assertions
						.assertTrue(c.getType().equals(eu.dnetlib.dhp.oa.graph.dump.Constants.RESEARCH_INFRASTRUCTURE));
					Assertions
						.assertTrue(
							c
								.getId()
								.equals(
									String
										.format(
											"%s|%s::%s", Constants.CONTEXT_ID, Constants.CONTEXT_NS_PREFIX,
											DHPUtils.md5(c.getOriginalId()))));
					Assertions.assertTrue(c.getZenodo_community().equals("https://zenodo.org/communities/oac_clarin"));
					Assertions.assertTrue("clarin".equals(c.getOriginalId()));
					break;
			}
			// TODO add check for all the others Entities

		});

		riList.forEach(c -> System.out.println(new Gson().toJson(c)));
	}

	@Test
	@Disabled
	public void test2() throws IOException, ISLookUpException {
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());

		Path hdfsWritePath = new Path(workingDir + "/prova");
		FSDataOutputStream fsDataOutputStream = null;
		if (fs.exists(hdfsWritePath)) {
			fsDataOutputStream = fs.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fs.create(hdfsWritePath);
		}
		CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
		CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec");

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(codec.createOutputStream(fsDataOutputStream),
			StandardCharsets.UTF_8));

		List<ContextInfo> cInfoList = new ArrayList<>();
		final Consumer<ContextInfo> consumer = ci -> cInfoList.add(ci);
		queryInformationSystem.getContextInformation(consumer);

		cInfoList.forEach(cInfo -> {
			try {
				writer.write(new Gson().toJson(Process.getEntity(cInfo)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		writer.close();

	}
}
