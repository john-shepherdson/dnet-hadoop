
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions.cleanup;
import static eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions.fixVocabularyNames;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.clean.CleaningRuleMap;
import eu.dnetlib.dhp.oa.graph.clean.OafCleaner;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
class MappersTest {

	@Mock
	private ISLookUpService isLookUpService;

	@Mock
	private VocabularyGroup vocs;

	@BeforeEach
	public void setUp() throws Exception {
		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());

		vocs = VocabularyGroup.loadVocsFromIS(isLookUpService);
	}

	@Test
	void testPublication() throws IOException {

		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_record.xml")));

		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.stream().filter(o -> o instanceof Publication).count());
		assertEquals(4, list.stream().filter(o -> o instanceof Relation).count());

		Publication p = (Publication) list.stream().filter(o -> o instanceof Publication).findFirst().get();

		assertValidId(p.getId());

		assertEquals(2, p.getOriginalId().size());
		assertTrue(p.getOriginalId().contains("10.3897/oneeco.2.e13718"));

		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertFalse(p.getDataInfo().getInvisible());
		assertEquals(1, p.getSource().size());
		assertTrue(StringUtils.isNotBlank(p.getDateofcollection()));
		assertTrue(StringUtils.isNotBlank(p.getDateoftransformation()));

		assertFalse(p.getAuthor().isEmpty());
		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.findFirst();
		assertTrue(author.isPresent());

		final StructuredProperty pid = author
			.get()
			.getPid()
			.stream()
			.findFirst()
			.orElseThrow(() -> new IllegalStateException("missing author pid"));
		assertEquals("0000-0001-6651-1178", pid.getValue());
		assertEquals(ModelConstants.ORCID_PENDING, pid.getQualifier().getClassid());
		assertEquals(ModelConstants.ORCID_CLASSNAME, pid.getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemename());
		assertEquals("Votsi,Nefta", author.get().getFullname());
		assertEquals("Votsi", author.get().getSurname());
		assertEquals("Nefta", author.get().getName());

		assertFalse(p.getSubject().isEmpty());
		assertTrue(StringUtils.isNotBlank(p.getJournal().getIssnOnline()));
		assertTrue(StringUtils.isNotBlank(p.getJournal().getName()));

		assertTrue(p.getPid().isEmpty());

		assertNotNull(p.getInstance());
		assertFalse(p.getInstance().isEmpty());
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		final Instance instance = p.getInstance().get(0);
		assertEquals("0001", instance.getRefereed().getClassid());
		assertNotNull(instance.getPid());
		assertTrue(instance.getPid().isEmpty());

		assertNotNull(instance.getInstanceTypeMapping());
		assertEquals(1, instance.getInstanceTypeMapping().size());

		Optional<InstanceTypeMapping> coarType = instance
			.getInstanceTypeMapping()
			.stream()
			.filter(itm -> ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(itm.getVocabularyName()))
			.findFirst();

		assertTrue(coarType.isPresent());
		assertNull(coarType.get().getTypeCode());
		assertNull(coarType.get().getTypeLabel());

		Optional<InstanceTypeMapping> userType = instance
			.getInstanceTypeMapping()
			.stream()
			.filter(itm -> ModelConstants.OPENAIRE_USER_RESOURCE_TYPES.equals(itm.getVocabularyName()))
			.findFirst();

		assertFalse(userType.isPresent());

		assertFalse(instance.getAlternateIdentifier().isEmpty());
		assertEquals("doi", instance.getAlternateIdentifier().get(0).getQualifier().getClassid());
		assertEquals("10.3897/oneeco.2.e13718", instance.getAlternateIdentifier().get(0).getValue());

		assertNotNull(instance.getFulltext());
		assertEquals("https://oneecosystem.pensoft.net/article/13718/", instance.getFulltext());

		assertNotNull(p.getBestaccessright());
		assertEquals("OPEN", p.getBestaccessright().getClassid());

		assertNotNull(p.getFulltext());
		assertEquals(1, p.getFulltext().size());
		assertEquals("https://oneecosystem.pensoft.net/article/13718/", p.getFulltext().get(0).getValue());

		// RESULT PROJECT
		List<Relation> resultProject = list
			.stream()
			.filter(o -> o instanceof Relation)
			.map(o -> (Relation) o)
			.filter(r -> ModelConstants.RESULT_PROJECT.equals(r.getRelType()))
			.collect(Collectors.toList());

		assertEquals(2, resultProject.size());
		final Relation rp1 = resultProject.get(0);
		final Relation rp2 = resultProject.get(1);

		verifyRelation(rp1);
		verifyRelation(rp2);

		assertTrue(rp1.getValidated());
		assertTrue(rp2.getValidated());
		assertEquals("2020-01-01", rp1.getValidationDate());
		assertEquals("2020-01-01", rp2.getValidationDate());

		assertEquals(rp1.getSource(), rp2.getTarget());
		assertEquals(rp2.getSource(), rp1.getTarget());

		// AFFILIATIONS
		List<Relation> affiliation = list
			.stream()
			.filter(o -> o instanceof Relation)
			.map(o -> (Relation) o)
			.filter(r -> ModelConstants.RESULT_ORGANIZATION.equals(r.getRelType()))
			.collect(Collectors.toList());

		assertEquals(2, affiliation.size());
		final Relation aff1 = affiliation.get(0);
		final Relation aff2 = affiliation.get(1);

		verifyRelation(aff1);
		verifyRelation(aff2);

		assertEquals(aff1.getSource(), aff2.getTarget());
		assertEquals(aff2.getSource(), aff1.getTarget());

		// COUNTRIES
		assertEquals(3, p.getCountry().size());
		assertEquals("IT", p.getCountry().get(0).getClassid());
		assertEquals("FR", p.getCountry().get(1).getClassid());
		assertEquals("DE", p.getCountry().get(2).getClassid());
	}

	private void verifyRelation(Relation r) {
		assertValidId(r.getSource());
		assertValidId(r.getTarget());
		assertValidId(r.getCollectedfrom().get(0).getKey());
		assertNotNull(r.getDataInfo());
		assertNotNull(r.getDataInfo().getTrust());
		assertTrue(StringUtils.isNotBlank(r.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r.getRelType()));

	}

	@Test
	void testPublication_PubMed() throws IOException {

		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_record_pubmed.xml")));

		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertValidId(p.getId());

		assertEquals(2, p.getOriginalId().size());
		assertTrue(p.getOriginalId().contains("oai:pubmedcentral.nih.gov:1517292"));

		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertFalse(p.getDataInfo().getInvisible());
		assertTrue(StringUtils.isNotBlank(p.getDateofcollection()));
		assertTrue(StringUtils.isNotBlank(p.getDateoftransformation()));

		assertFalse(p.getAuthor().isEmpty());
		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.findFirst();
		assertTrue(author.isPresent());

		final StructuredProperty pid = author
			.get()
			.getPid()
			.stream()
			.findFirst()
			.get();
		assertEquals("0000-0001-6651-1178", pid.getValue());
		assertEquals(ModelConstants.ORCID_PENDING, pid.getQualifier().getClassid());
		assertEquals(ModelConstants.ORCID_CLASSNAME, pid.getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemename());
		assertEquals("Votsi,Nefta", author.get().getFullname());
		assertEquals("Votsi", author.get().getSurname());
		assertEquals("Nefta", author.get().getName());

		assertFalse(p.getSubject().isEmpty());
		assertFalse(p.getPid().isEmpty());

		assertTrue(p.getPid().stream().anyMatch(pi -> "pmc".equals(pi.getQualifier().getClassid())));
		assertEquals(
			"PMC1517292",
			p
				.getPid()
				.stream()
				.filter(pi -> "pmc".equals(pi.getQualifier().getClassid()))
				.findFirst()
				.get()
				.getValue());

		assertNotNull(p.getInstance());
		assertFalse(p.getInstance().isEmpty());
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});

		Publication p_cleaned = cleanup(p, vocs);
		assertEquals("0002", p_cleaned.getInstance().get(0).getRefereed().getClassid());
		assertEquals("nonPeerReviewed", p_cleaned.getInstance().get(0).getRefereed().getClassname());

		assertNotNull(p.getInstance().get(0).getPid());
		assertEquals(2, p.getInstance().get(0).getPid().size());

		assertEquals(1, p.getInstance().get(0).getAlternateIdentifier().size());
		assertEquals("doi", p.getInstance().get(0).getAlternateIdentifier().get(0).getQualifier().getClassid());
		assertEquals("10.3897/oneeco.2.e13718", p.getInstance().get(0).getAlternateIdentifier().get(0).getValue());

		assertNotNull(p.getBestaccessright());
		assertEquals("OPEN", p.getBestaccessright().getClassid());
	}

	@Test
	void testPublicationInvisible() throws IOException {

		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_record.xml")));

		final List<Oaf> list = new OafToOafMapper(vocs, true, true).processMdRecord(xml);

		assertFalse(list.isEmpty());
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertTrue(p.getDataInfo().getInvisible());

	}

	@Test
	void testPublicationInvisible_BASE() throws IOException {

		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_record_base.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, true, true).processMdRecord(xml);

		assertFalse(list.isEmpty());
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertTrue(p.getDataInfo().getInvisible());

		System.out.println(new ObjectMapper().writeValueAsString(p));

	}

	@Test
	void testOdfFwfEBookLibrary() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_fwfebooklibrary.xml")));

		assertThrows(
			IllegalArgumentException.class,
			() -> new OdfToOafMapper(vocs, false, true).processMdRecord(xml));
	}

	@Test
	void testDataset() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_dataset.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Dataset);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Dataset d = (Dataset) list.get(0);
		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertEquals(d.getId(), r1.getTarget());
		assertEquals("40|corda_______::e06332dee33bec6c2ba4c98601053229", r1.getSource());
		assertEquals(ModelConstants.RESULT_PROJECT, r1.getRelType());
		assertEquals(ModelConstants.OUTCOME, r1.getSubRelType());
		assertEquals(ModelConstants.PRODUCES, r1.getRelClass());
		assertTrue(r1.getValidated());
		assertEquals("2020-01-01", r1.getValidationDate());

		assertEquals(d.getId(), r2.getSource());
		assertEquals("40|corda_______::e06332dee33bec6c2ba4c98601053229", r2.getTarget());
		assertEquals(ModelConstants.RESULT_PROJECT, r2.getRelType());
		assertEquals(ModelConstants.OUTCOME, r2.getSubRelType());
		assertEquals(ModelConstants.IS_PRODUCED_BY, r2.getRelClass());
		assertTrue(r2.getValidated());
		assertEquals("2020-01-01", r2.getValidationDate());

		assertValidId(d.getId());
		assertEquals("50|doi_________::000374d100a9db469bd42b69dbb40b36", d.getId());
		assertEquals(2, d.getOriginalId().size());
		assertTrue(d.getOriginalId().stream().anyMatch(oid -> oid.equals("oai:zenodo.org:3234526")));
		assertValidId(d.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertFalse(d.getAuthor().isEmpty());

		final Optional<Author> author = d
			.getAuthor()
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.findFirst();
		assertTrue(author.isPresent());
		final Optional<StructuredProperty> oPid = author
			.get()
			.getPid()
			.stream()
			.findFirst();
		assertTrue(oPid.isPresent());
		final StructuredProperty pid = oPid.get();
		assertEquals("0000-0001-9074-1619", pid.getValue());
		assertEquals(ModelConstants.ORCID_PENDING, pid.getQualifier().getClassid());
		assertEquals(ModelConstants.ORCID_CLASSNAME, pid.getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemename());
		assertEquals("Baracchini, Theo", author.get().getFullname());
		assertEquals("Baracchini", author.get().getSurname());
		assertEquals("Theo", author.get().getName());

		assertEquals(1, author.get().getAffiliation().size());
		final Optional<Field<String>> opAff = author
			.get()
			.getAffiliation()
			.stream()
			.findFirst();
		assertTrue(opAff.isPresent());
		final Field<String> affiliation = opAff.get();
		assertEquals("ISTI-CNR", affiliation.getValue());

		assertFalse(d.getSubject().isEmpty());
		assertFalse(d.getInstance().isEmpty());
		assertFalse(d.getContext().isEmpty());
		assertFalse(d.getContext().get(0).getId().isEmpty());

		assertNotNull(d.getInstance());
		assertFalse(d.getInstance().isEmpty());
		d
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("0001", d.getInstance().get(0).getRefereed().getClassid());
		assertNotNull(d.getInstance().get(0).getPid());
		assertFalse(d.getInstance().get(0).getPid().isEmpty());

		assertEquals("doi", d.getInstance().get(0).getPid().get(0).getQualifier().getClassid());
		assertEquals("10.5281/zenodo.3234526", d.getInstance().get(0).getPid().get(0).getValue());

		assertTrue(d.getInstance().get(0).getAlternateIdentifier().isEmpty());

		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertNotNull(r1.getDataInfo());
		assertNotNull(r2.getDataInfo());
		assertNotNull(r1.getDataInfo().getTrust());
		assertNotNull(r2.getDataInfo().getTrust());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));
		assertTrue(r1.getValidated());
		assertTrue(r2.getValidated());
		assertEquals("2020-01-01", r1.getValidationDate());
		assertEquals("2020-01-01", r2.getValidationDate());

		assertNotNull(d.getTitle());
		assertEquals(2, d.getTitle().size());
		verifyTitle(d, "main title", "Temperature and ADCP data collected on Lake Geneva between 2015 and 2017");
		verifyTitle(d, "Subtitle", "survey");
	}

	private void verifyTitle(Dataset d, String titleType, String title) {
		Optional
			.of(
				d
					.getTitle()
					.stream()
					.filter(t -> titleType.equals(t.getQualifier().getClassid()))
					.collect(Collectors.toList()))
			.ifPresent(t -> {
				assertEquals(1, t.size());
				assertEquals(title, t.get(0).getValue());
			});
	}

	@Test
	void testOdfBielefeld() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_bielefeld.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertValidId(p.getId());
		assertEquals(2, p.getOriginalId().size());

		assertTrue(p.getOriginalId().stream().anyMatch(oid -> oid.equals("oai:pub.uni-bielefeld.de:2949739")));
		// assertEquals("oai:pub.uni-bielefeld.de:2949739", p.getOriginalId().get(0));

		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertFalse(p.getAuthor().isEmpty());

		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.findFirst();
		assertTrue(author.isPresent());

		assertEquals("Potwarka, Luke R.", author.get().getFullname());
		assertEquals("Potwarka", author.get().getSurname());
		assertEquals("Luke R.", author.get().getName());

		assertFalse(p.getSubject().isEmpty());
		assertFalse(p.getInstance().isEmpty());

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());

		assertNotNull(p.getInstance());
		assertFalse(p.getInstance().isEmpty());
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});

		Publication p_cleaned = cleanup(p, vocs);
		assertEquals("0002", p_cleaned.getInstance().get(0).getRefereed().getClassid());
		assertEquals("nonPeerReviewed", p_cleaned.getInstance().get(0).getRefereed().getClassname());
	}

	@Test
	void testOpentrial() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_opentrial.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Dataset);
		final Dataset d = (Dataset) list.get(0);

		assertNotNull(d.getDateofcollection());
		assertEquals("2019-03-27T15:15:22.22Z", d.getDateofcollection());

		assertNotNull(d.getDateoftransformation());
		assertEquals("2019-04-17T16:04:20.586Z", d.getDateoftransformation());

		assertNotNull(d.getDataInfo());
		assertFalse(d.getDataInfo().getInvisible());
		assertFalse(d.getDataInfo().getDeletedbyinference());
		assertEquals("0.9", d.getDataInfo().getTrust());

		assertEquals("", d.getDataInfo().getInferenceprovenance());

		assertEquals("sysimport:crosswalk:datasetarchive", d.getDataInfo().getProvenanceaction().getClassid());
		assertEquals("sysimport:crosswalk:datasetarchive", d.getDataInfo().getProvenanceaction().getClassname());
		assertEquals(ModelConstants.DNET_PROVENANCE_ACTIONS, d.getDataInfo().getProvenanceaction().getSchemeid());
		assertEquals(ModelConstants.DNET_PROVENANCE_ACTIONS, d.getDataInfo().getProvenanceaction().getSchemename());

		assertValidId(d.getId());
		assertEquals(2, d.getOriginalId().size());

		assertEquals("feabb67c-1fd1-423b-aec6-606d04ce53c6", d.getOriginalId().get(0));
		assertValidId(d.getCollectedfrom().get(0).getKey());

		assertNotNull(d.getTitle());
		assertEquals(1, d.getTitle().size());
		assertEquals(
			"Validation of the Goodstrength System for Assessment of Abdominal Wall Strength in Patients With Incisional Hernia",
			d
				.getTitle()
				.get(0)
				.getValue());

		assertNotNull(d.getDescription());
		assertEquals(1, d.getDescription().size());
		assertTrue(StringUtils.isNotBlank(d.getDescription().get(0).getValue()));

		assertEquals(1, d.getAuthor().size());
		assertEquals("Jensen, Kristian K", d.getAuthor().get(0).getFullname());
		assertEquals("Kristian K.", d.getAuthor().get(0).getName());
		assertEquals("Jensen", d.getAuthor().get(0).getSurname());

		assertNotNull(d.getAuthor().get(0).getPid());
		assertTrue(d.getAuthor().get(0).getPid().isEmpty());

		assertNotNull(d.getPid());
		assertEquals(0, d.getPid().size());

		assertNotNull(d.getPublisher());
		assertEquals("nct", d.getPublisher().getValue());

		assertTrue(d.getSubject().isEmpty());
		assertTrue(d.getContext().isEmpty());

		assertNotNull(d.getInstance());
		assertEquals(1, d.getInstance().size());

		final Instance i = d.getInstance().get(0);

		assertNotNull(i.getAccessright());
		assertEquals(ModelConstants.DNET_ACCESS_MODES, i.getAccessright().getSchemeid());
		assertEquals(ModelConstants.DNET_ACCESS_MODES, i.getAccessright().getSchemename());
		assertEquals("OPEN", i.getAccessright().getClassid());
		assertEquals("Open Access", i.getAccessright().getClassname());

		assertNotNull(i.getCollectedfrom());
		assertEquals("10|openaire____::b292fc2d7de505f78e3cae1b06ea8548", i.getCollectedfrom().getKey());
		assertEquals("OpenTrials", i.getCollectedfrom().getValue());

		assertNotNull(i.getHostedby());
		assertEquals("10|openaire____::b292fc2d7de505f78e3cae1b06ea8548", i.getHostedby().getKey());
		assertEquals("OpenTrials", i.getHostedby().getValue());

		assertNotNull(i.getInstancetype());
		assertEquals("0037", i.getInstancetype().getClassid());
		assertEquals("Clinical Trial", i.getInstancetype().getClassname());
		assertEquals(ModelConstants.DNET_PUBLICATION_RESOURCE, i.getInstancetype().getSchemeid());
		assertEquals(ModelConstants.DNET_PUBLICATION_RESOURCE, i.getInstancetype().getSchemename());

		assertNull(i.getLicense());
		assertNotNull(i.getDateofacceptance());
		assertEquals("2014-11-11", i.getDateofacceptance().getValue());

		assertNull(i.getDistributionlocation());
		assertNull(i.getProcessingchargeamount());
		assertNull(i.getProcessingchargecurrency());

		assertNotNull(i.getPid());
		assertEquals(0, i.getPid().size());

		assertNotNull(i.getAlternateIdentifier());
		assertEquals(1, i.getAlternateIdentifier().size());
		assertEquals("NCT02321059", i.getAlternateIdentifier().get(0).getValue());
		assertEquals("nct", i.getAlternateIdentifier().get(0).getQualifier().getClassid());
		assertEquals("ClinicalTrials.gov Identifier", i.getAlternateIdentifier().get(0).getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, i.getAlternateIdentifier().get(0).getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, i.getAlternateIdentifier().get(0).getQualifier().getSchemename());

		assertNotNull(i.getUrl());
		assertEquals(2, i.getUrl().size());
		assertTrue(i.getUrl().contains("http://apps.who.int/trialsearch/Trial3.aspx?trialid=NCT02321059"));
		assertTrue(i.getUrl().contains("https://clinicaltrials.gov/ct2/show/NCT02321059"));

		Dataset d_cleaned = cleanup(d, vocs);
		assertEquals("0002", d_cleaned.getInstance().get(0).getRefereed().getClassid());
		assertEquals("nonPeerReviewed", d_cleaned.getInstance().get(0).getRefereed().getClassname());
	}

	@Test
	void test_record_from_Crossref() throws IOException {

		final CleaningRuleMap mapping = CleaningRuleMap.create(vocs);

		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_crossref.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = OafCleaner.apply(fixVocabularyNames((Publication) list.get(0)), mapping);

		assertNotNull(p.getDateofcollection());
		assertEquals("2020-08-06T07:04:09.62Z", p.getDateofcollection());

		assertNotNull(p.getDateoftransformation());
		assertEquals("2020-08-06T07:20:57.911Z", p.getDateoftransformation());

		assertNotNull(p.getDataInfo());
		assertFalse(p.getDataInfo().getInvisible());
		assertFalse(p.getDataInfo().getDeletedbyinference());
		assertEquals("0.9", p.getDataInfo().getTrust());

		assertValidId(p.getId());
		assertEquals(2, p.getOriginalId().size());

		assertEquals("50|doi_________::7f0f7807f17db50e5c2b5c452ccaf06d", p.getOriginalId().get(0));
		assertValidId(p.getCollectedfrom().get(0).getKey());

		assertNotNull(p.getTitle());
		assertEquals(1, p.getTitle().size());
		assertEquals(
			"A case report of serious haemolysis in a glucose-6-phosphate dehydrogenase-deficient COVID-19 patient receiving hydroxychloroquine",
			p
				.getTitle()
				.get(0)
				.getValue());

		assertNotNull(p.getDescription());
		assertEquals(0, p.getDescription().size());

		assertEquals(8, p.getAuthor().size());

		assertNotNull(p.getInstance());
		assertEquals(1, p.getInstance().size());

		final Instance i = p.getInstance().get(0);

		assertNotNull(i.getAccessright());
		assertEquals(ModelConstants.DNET_ACCESS_MODES, i.getAccessright().getSchemeid());
		assertEquals(ModelConstants.DNET_ACCESS_MODES, i.getAccessright().getSchemename());
		assertEquals("OPEN", i.getAccessright().getClassid());
		assertEquals("Open Access", i.getAccessright().getClassname());

		assertNotNull(i.getCollectedfrom());
		assertEquals("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2", i.getCollectedfrom().getKey());
		assertEquals("Crossref", i.getCollectedfrom().getValue());

		assertNotNull(i.getHostedby());
		assertEquals("10|openaire____::55045bd2a65019fd8e6741a755395c8c", i.getHostedby().getKey());
		assertEquals("Unknown Repository", i.getHostedby().getValue());

		assertNotNull(i.getInstancetype());
		assertEquals("0001", i.getInstancetype().getClassid());
		assertEquals("Article", i.getInstancetype().getClassname());
		assertEquals(ModelConstants.DNET_PUBLICATION_RESOURCE, i.getInstancetype().getSchemeid());
		assertEquals(ModelConstants.DNET_PUBLICATION_RESOURCE, i.getInstancetype().getSchemename());

		assertNull(i.getLicense());
		assertNotNull(i.getDateofacceptance());
		assertEquals("2020-06-04", i.getDateofacceptance().getValue());

		assertNull(i.getProcessingchargeamount());
		assertNull(i.getProcessingchargecurrency());

		assertNotNull(i.getPid());
		assertEquals(1, i.getPid().size());

		assertNotNull(i.getAlternateIdentifier());
		assertEquals(0, i.getAlternateIdentifier().size());

		assertNotNull(i.getUrl());
		assertEquals(1, i.getUrl().size());
		assertTrue(i.getUrl().contains("http://dx.doi.org/10.1080/23744235.2020.1774644"));

		assertEquals("", p.getInstance().get(0).getRefereed().getClassid());
		assertEquals("", p.getInstance().get(0).getRefereed().getClassname());

		Publication p_cleaned = cleanup(p, vocs);

		assertEquals("0001", p_cleaned.getInstance().get(0).getRefereed().getClassid());
		assertEquals("peerReviewed", p_cleaned.getInstance().get(0).getRefereed().getClassname());

		assertNull(p_cleaned.getMetaResourceType());

		assertNotNull(p_cleaned.getInstance().get(0).getInstanceTypeMapping());
		assertEquals(1, p_cleaned.getInstance().get(0).getInstanceTypeMapping().size());

		assertTrue(
			p_cleaned
				.getInstance()
				.get(0)
				.getInstanceTypeMapping()
				.stream()
				.anyMatch(
					t -> "journal-article".equals(t.getOriginalType()) &&
						ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(t.getVocabularyName()) &&
						Objects.isNull(t.getTypeCode()) && Objects.isNull(t.getTypeLabel())));

		assertTrue(
			p_cleaned
				.getInstance()
				.get(0)
				.getInstanceTypeMapping()
				.stream()
				.noneMatch(
					t -> ModelConstants.OPENAIRE_USER_RESOURCE_TYPES.equals(t.getVocabularyName())));
	}

	@Test
	void testSoftware() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_software.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Software);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Software s = (Software) list.get(0);

		assertValidId(s.getId());
		assertValidId(s.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(s.getTitle().get(0).getValue()));
		assertFalse(s.getAuthor().isEmpty());
		assertFalse(s.getSubject().isEmpty());
		assertFalse(s.getInstance().isEmpty());

		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertEquals(s.getId(), r1.getTarget());
		assertEquals("50|doi_________::b453e7b4b2130ace57ff0c3db470a982", r1.getSource());
		assertEquals(ModelConstants.RESULT_RESULT, r1.getRelType());
		assertEquals(ModelConstants.RELATIONSHIP, r1.getSubRelType());
		assertEquals(ModelConstants.REFERENCES, r1.getRelClass());

		assertEquals(s.getId(), r2.getSource());
		assertEquals("50|doi_________::b453e7b4b2130ace57ff0c3db470a982", r2.getTarget());
		assertEquals(ModelConstants.RESULT_RESULT, r2.getRelType());
		assertEquals(ModelConstants.RELATIONSHIP, r2.getSubRelType());
		assertEquals(ModelConstants.IS_REFERENCED_BY, r2.getRelClass());
	}

	@Test
	void testClaimDedup() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_claim_dedup.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		assertNotNull(list);
		assertFalse(list.isEmpty());

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
	}

	@Test
	void testNakala() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_nakala.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Dataset);

		final Dataset d = (Dataset) list.get(0);

		assertValidId(d.getId());
		assertValidId(d.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertEquals(1, d.getAuthor().size());
		assertEquals(1, d.getSubject().size());
		assertEquals(1, d.getInstance().size());
		assertNotNull(d.getPid());
		assertEquals(1, d.getPid().size());
		assertTrue(PidType.isValid(d.getPid().get(0).getQualifier().getClassid()));
		assertEquals(PidType.handle, PidType.valueOf(d.getPid().get(0).getQualifier().getClassid()));

		assertNotNull(d.getInstance().get(0).getUrl());
	}

	@Test
	void testEnermaps() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("enermaps.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Dataset);

		final Dataset d = (Dataset) list.get(0);

		assertValidId(d.getId());
		assertValidId(d.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertEquals(1, d.getAuthor().size());
		assertEquals(1, d.getInstance().size());
		assertNotNull(d.getInstance().get(0).getUrl());
		assertNotNull(d.getContext());
		assertTrue(StringUtils.isNotBlank(d.getContext().get(0).getId()));
		assertEquals("enermaps::selection::tgs00004", d.getContext().get(0).getId());
	}

	@Test
	void testClaimFromCrossref() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_claim_crossref.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Publication p = (Publication) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		System.out.println(p.getTitle().get(0).getValue());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
	}

	@Test
	void testODFRecord() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_record.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
		final Dataset p = (Dataset) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		System.out.println(p.getTitle().get(0).getValue());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));

		// COUNTRIES
		assertEquals(3, p.getCountry().size());
		assertEquals("IT", p.getCountry().get(0).getClassid());
		assertEquals("FR", p.getCountry().get(1).getClassid());
		assertEquals("DE", p.getCountry().get(2).getClassid());
	}

	@Test
	void testTextGrid() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("textgrid.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Dataset p = (Dataset) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertEquals(1, p.getAuthor().size());
		assertEquals("OPEN", p.getBestaccessright().getClassid());

		assertEquals(1, p.getPid().size());
		assertTrue(PidType.isValid(p.getPid().get(0).getQualifier().getClassid()));
		assertEquals(PidType.handle, PidType.valueOf(p.getPid().get(0).getQualifier().getClassid()));
		assertEquals("hdl:11858/00-1734-0000-0003-EE73-2", p.getPid().get(0).getValue());
		assertEquals("dataset", p.getResulttype().getClassname());
		assertEquals(1, p.getInstance().size());
		assertEquals("OPEN", p.getInstance().get(0).getAccessright().getClassid());
		assertValidId(p.getInstance().get(0).getCollectedfrom().getKey());
		assertValidId(p.getInstance().get(0).getHostedby().getKey());
		assertEquals(
			"http://creativecommons.org/licenses/by/3.0/de/legalcode", p.getInstance().get(0).getLicense().getValue());

		assertEquals(1, p.getInstance().size());
		assertNotNull(p.getInstance().get(0).getAlternateIdentifier());
		assertEquals(0, p.getInstance().get(0).getAlternateIdentifier().size());
		assertEquals(1, p.getInstance().get(0).getUrl().size());
	}

	@Test
	void testBologna() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf-bologna.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Publication p = (Publication) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		System.out.println(p.getTitle().get(0).getValue());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		System.out.println(p.getTitle().get(0).getValue());
	}

	@Test
	void testJairo() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_jairo.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Publication p = (Publication) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());
		assertEquals(1, p.getTitle().size());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));

		final Publication p_cleaned = cleanup(fixVocabularyNames(p), vocs);

		assertNotNull(p_cleaned.getTitle());
		assertFalse(p_cleaned.getTitle().isEmpty());
	}

	@Test
	void test_instance_url_validation() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("idus_sevilla.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		final Publication p = (Publication) list.get(0);

		assertNotNull(p.getInstance());
		assertFalse(p.getInstance().isEmpty());
		assertNotNull(p.getInstance().get(0).getUrl());
		assertFalse(p.getInstance().get(0).getUrl().isEmpty());
		assertEquals("https://idus.us.es/handle//11441/118940", p.getInstance().get(0).getUrl().get(0));
	}

	@Test
	void testZenodo() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_zenodo.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Publication p = (Publication) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());
		assertEquals(1, p.getTitle().size());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));

		assertNotNull(p.getAuthor());
		assertEquals(2, p.getAuthor().size());

		Author author = p
			.getAuthor()
			.stream()
			.filter(a -> a.getPid().stream().anyMatch(pi -> pi.getValue().equals("0000-0003-3272-8007")))
			.findFirst()
			.get();
		assertNotNull(author);
		assertTrue(StringUtils.isBlank(author.getSurname()));
		assertTrue(StringUtils.isBlank(author.getName()));
		assertEquals("Anne van Weerden", author.getFullname());

		author = p
			.getAuthor()
			.stream()
			.filter(a -> a.getPid().stream().anyMatch(pi -> pi.getValue().equals("0000-0003-3272-8008")))
			.findFirst()
			.get();
		assertNotNull(author);
		assertFalse(StringUtils.isBlank(author.getSurname()));
		assertFalse(StringUtils.isBlank(author.getName()));
		assertFalse(StringUtils.isBlank(author.getFullname()));

	}

	@Test
	void testOdfFromHdfs() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_from_hdfs.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());

		System.out.println(list.get(0).getClass());

		assertTrue(list.get(0) instanceof Dataset);

		final Dataset p = (Dataset) list.get(0);

		assertValidId(p.getId());
		assertEquals(2, p.getOriginalId().size());
		assertTrue(p.getOriginalId().stream().anyMatch(oid -> oid.equals("df76e73f-0483-49a4-a9bb-63f2f985574a")));
		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertFalse(p.getAuthor().isEmpty());

		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.findFirst();
		assertTrue(author.isPresent());

		assertEquals("Museum Sønderjylland", author.get().getFullname());

		assertFalse(p.getSubject().isEmpty());
		assertFalse(p.getInstance().isEmpty());

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());

		assertNotNull(p.getInstance());
		assertFalse(p.getInstance().isEmpty());
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("UNKNOWN", i.getAccessright().getClassid());
			});

		Dataset p_cleaned = cleanup(p, vocs);
		assertEquals("0002", p_cleaned.getInstance().get(0).getRefereed().getClassid());
		assertEquals("nonPeerReviewed", p_cleaned.getInstance().get(0).getRefereed().getClassname());
	}

	@Test
	void testXMLEncodedURL() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("encoded-url.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Publication p = (Publication) list.get(0);
		assertTrue(p.getInstance().size() > 0);

		String decoded = "https://www.ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5af388993&appId=PPGMS";
		assertEquals(decoded, p.getInstance().get(0).getUrl().get(0));
	}

	@Test
	void testXMLEncodedURL_ODF() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("encoded-url_odf.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Dataset p = (Dataset) list.get(0);
		assertFalse(p.getInstance().isEmpty());
		for (String url : p.getInstance().get(0).getUrl()) {
			System.out.println(url);
			assertFalse(url.contains("&amp;"));
		}
	}

	@Test
	void testOpenAPC() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_openapc.xml")));
		final List<Oaf> list = new OafToOafMapper(vocs, true, true).processMdRecord(xml);

		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");

		final Optional<Oaf> o = list.stream().filter(r -> r instanceof Publication).findFirst();
		assertTrue(o.isPresent());

		Publication p = (Publication) o.get();
		assertFalse(p.getInstance().isEmpty());

		assertEquals("https://doi.org/10.1155/2015/439379", p.getInstance().get(0).getUrl().get(0));

		assertNotNull(p.getProcessingchargeamount());
		assertNotNull(p.getProcessingchargecurrency());

		assertEquals("1721.47", p.getProcessingchargeamount().getValue());
		assertEquals("EUR", p.getProcessingchargecurrency().getValue());

		List<Oaf> affiliations = list.stream().filter(r -> r instanceof Relation).collect(Collectors.toList());
		assertEquals(2, affiliations.size());

		for (Oaf aff : affiliations) {
			Relation r = (Relation) aff;
			assertEquals(ModelConstants.AFFILIATION, r.getSubRelType());
			assertEquals(ModelConstants.RESULT_ORGANIZATION, r.getRelType());
			String source = r.getSource();
			if (StringUtils.startsWith(source, "50")) {
				assertEquals(ModelConstants.HAS_AUTHOR_INSTITUTION, r.getRelClass());
			} else if (StringUtils.startsWith(source, "20")) {
				assertTrue(StringUtils.contains(source, "::"));
				assertEquals("20|" + Constants.ROR_NS_PREFIX, StringUtils.substringBefore(source, "::"));
				assertEquals(ModelConstants.IS_AUTHOR_INSTITUTION_OF, r.getRelClass());
			} else {
				throw new IllegalArgumentException("invalid source / target prefixes for affiliation relations");
			}

			List<KeyValue> apcInfo = r.getProperties();
			assertEquals(
				"EUR", apcInfo
					.stream()
					.filter(kv -> "apc_currency".equals(kv.getKey()))
					.map(KeyValue::getValue)
					.findFirst()
					.orElse(""));
			assertEquals(
				"1721.47", apcInfo
					.stream()
					.filter(kv -> "apc_amount".equals(kv.getKey()))
					.map(KeyValue::getValue)
					.findFirst()
					.orElse(""));
		}
	}

	@Test
	void testROHub() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("rohub.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
		assertEquals(5, list.size());
		final OtherResearchProduct p = (OtherResearchProduct) list.get(0);
		assertValidId(p.getId());
		assertTrue(p.getId().startsWith("50|w3id"));
		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertEquals(1, p.getInstance().size());
		assertEquals("https://w3id.org/ro-id/0ab171a7-45c5-4194-82d4-850955504bca", p.getPid().get(0).getValue());
		Instance inst = p.getInstance().get(0);
		assertEquals("https://w3id.org/ro-id/0ab171a7-45c5-4194-82d4-850955504bca", inst.getPid().get(0).getValue());
		assertEquals("https://w3id.org/ro-id/0ab171a7-45c5-4194-82d4-850955504bca", inst.getUrl().get(0));
		assertEquals(1, p.getEoscifguidelines().size());
		assertEquals("EOSC::RO-crate", p.getEoscifguidelines().get(0).getCode());
		assertEquals("EOSC::RO-crate", p.getEoscifguidelines().get(0).getLabel());
		assertEquals("", p.getEoscifguidelines().get(0).getUrl());
		assertEquals("compliesWith", p.getEoscifguidelines().get(0).getSemanticRelation());

	}

	@Test
	void test_Zenodo2() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_zenodo2.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(3, list.size());
		Publication p = cleanup((Publication) list.get(0), vocs);

		assertNotNull(p.getInstance());
		assertEquals(1, p.getInstance().size());

		final Instance instance = p.getInstance().get(0);

		assertNotNull(instance.getInstanceTypeMapping());
		assertEquals(1, instance.getInstanceTypeMapping().size());

		Optional<InstanceTypeMapping> coarType = instance
			.getInstanceTypeMapping()
			.stream()
			.filter(itm -> ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(itm.getVocabularyName()))
			.findFirst();

		assertTrue(coarType.isPresent());
		assertNotNull(coarType.get().getOriginalType());
		assertNull(coarType.get().getTypeCode());
		assertNull(coarType.get().getTypeLabel());
	}

	@Test
	void testROHub2() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("rohub-modified.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
		assertEquals(7, list.size());
		final OtherResearchProduct p = (OtherResearchProduct) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertEquals("50|w3id________::afc7592914ae190a50570db90f55f9c2", p.getId());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertEquals("w3id", (p.getPid().get(0).getQualifier().getClassid()));
		assertEquals("https://w3id.org/ro-id/0ab171a7-45c5-4194-82d4-850955504bca", (p.getPid().get(0).getValue()));

		assertEquals(1, list.stream().filter(o -> o instanceof OtherResearchProduct).count());
		assertEquals(6, list.stream().filter(o -> o instanceof Relation).count());

		for (Oaf oaf : list) {
			if (oaf instanceof Relation) {
				String source = ((Relation) oaf).getSource();
				String target = ((Relation) oaf).getTarget();
				assertNotEquals(source, target);
				assertTrue(source.equals(p.getId()) || target.equals(p.getId()));
				assertNotNull(((Relation) oaf).getSubRelType());
				assertNotNull(((Relation) oaf).getRelClass());
				assertNotNull(((Relation) oaf).getRelType());
			}
		}
	}

	@Test
	void testRiunet() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("riunet.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
		final Publication p = (Publication) list.get(0);
		assertNotNull(p.getInstance().get(0).getUrl().get(0));

	}

	@Test
	void testEOSCFuture_ROHub() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("photic-zone-transformed.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		final OtherResearchProduct rocrate = (OtherResearchProduct) list.get(0);
		assertNotNull(rocrate.getEoscifguidelines());
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(rocrate));
		System.out.println("***************");
	}

	@Test
	void testD4ScienceTraining() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("d4science-1-training.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		final OtherResearchProduct trainingMaterial = (OtherResearchProduct) list.get(0);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(trainingMaterial));
		System.out.println("***************");
	}

	@Test
	void testD4ScienceDataset() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("d4science-2-dataset.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		final Dataset trainingMaterial = (Dataset) list.get(0);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(trainingMaterial));
		System.out.println("***************");
	}

	@Test
	void testIRISPub() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("iris-odf.xml")));
		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);
		System.out.println("***************");
		System.out.println(new ObjectMapper().writeValueAsString(list));
		System.out.println("***************");
		final Publication p = (Publication) list.get(0);
		assertNotNull(p.getInstance().get(0).getUrl().get(0));
		assertValidId(p.getId());
		System.out.println(p.getInstance().get(0).getUrl());
		p.getPid().forEach(x -> System.out.println(x.getValue()));
		p.getInstance().get(0).getAlternateIdentifier().forEach(x -> System.out.println(x.getValue()));

	}

	@Test
	void testNotWellFormed() throws IOException {
		final String xml = IOUtils
			.toString(Objects.requireNonNull(getClass().getResourceAsStream("oaf_notwellformed.xml")));
		final List<Oaf> actual = new OafToOafMapper(vocs, false, true).processMdRecord(xml);
		assertNotNull(actual);
		assertTrue(actual.isEmpty());
	}

	private void assertValidId(final String id) {
		// System.out.println(id);

		assertEquals(49, id.length());
		assertEquals(IdentifierFactory.ID_PREFIX_SEPARATOR, id.substring(2, 3));
		assertEquals(IdentifierFactory.ID_SEPARATOR, id.substring(15, 17));
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(MappersTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt")));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(
						MappersTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt")));
	}

}
