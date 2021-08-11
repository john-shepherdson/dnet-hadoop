
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions.cleanup;
import static eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions.fixVocabularyNames;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
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

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Publication);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Publication p = (Publication) list.get(0);
		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertValidId(p.getId());

		assertEquals(2, p.getOriginalId().size());
		assertTrue(p.getOriginalId().contains("10.3897/oneeco.2.e13718"));

		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertFalse(p.getDataInfo().getInvisible());
		assertEquals(1, p.getSource().size());
		assertTrue(StringUtils.isNotBlank(p.getDateofcollection()));
		assertTrue(StringUtils.isNotBlank(p.getDateoftransformation()));

		assertTrue(p.getAuthor().size() > 0);
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

		assertTrue(p.getSubject().size() > 0);
		assertTrue(StringUtils.isNotBlank(p.getJournal().getIssnOnline()));
		assertTrue(StringUtils.isNotBlank(p.getJournal().getName()));

		assertTrue(p.getPid().isEmpty());

		assertNotNull(p.getInstance());
		assertTrue(p.getInstance().size() > 0);
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("0001", p.getInstance().get(0).getRefereed().getClassid());
		assertNotNull(p.getInstance().get(0).getPid());
		assertTrue(p.getInstance().get(0).getPid().isEmpty());

		assertTrue(!p.getInstance().get(0).getAlternateIdentifier().isEmpty());
		assertEquals("doi", p.getInstance().get(0).getAlternateIdentifier().get(0).getQualifier().getClassid());
		assertEquals("10.3897/oneeco.2.e13718", p.getInstance().get(0).getAlternateIdentifier().get(0).getValue());

		assertNotNull(p.getBestaccessright());
		assertEquals("OPEN", p.getBestaccessright().getClassid());
		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertValidId(r1.getCollectedfrom().get(0).getKey());
		assertValidId(r2.getCollectedfrom().get(0).getKey());
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

		assertTrue(p.getAuthor().size() > 0);
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

		assertTrue(p.getSubject().size() > 0);
		assertTrue(p.getPid().size() > 0);
		assertEquals("PMC1517292", p.getPid().get(0).getValue());
		assertEquals("pmc", p.getPid().get(0).getQualifier().getClassid());

		assertNotNull(p.getInstance());
		assertTrue(p.getInstance().size() > 0);
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("UNKNOWN", p.getInstance().get(0).getRefereed().getClassid());
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

		assertTrue(list.size() > 0);
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertTrue(p.getDataInfo().getInvisible());

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

		assertEquals(d.getId(), r1.getSource());
		assertEquals("40|corda_______::e06332dee33bec6c2ba4c98601053229", r1.getTarget());
		assertEquals(ModelConstants.RESULT_PROJECT, r1.getRelType());
		assertEquals(ModelConstants.OUTCOME, r1.getSubRelType());
		assertEquals(ModelConstants.IS_PRODUCED_BY, r1.getRelClass());
		assertTrue(r1.getValidated());
		assertEquals("2020-01-01", r1.getValidationDate());

		assertEquals(d.getId(), r2.getTarget());
		assertEquals("40|corda_______::e06332dee33bec6c2ba4c98601053229", r2.getSource());
		assertEquals(ModelConstants.RESULT_PROJECT, r2.getRelType());
		assertEquals(ModelConstants.OUTCOME, r2.getSubRelType());
		assertEquals(ModelConstants.PRODUCES, r2.getRelClass());
		assertTrue(r2.getValidated());
		assertEquals("2020-01-01", r2.getValidationDate());

		assertValidId(d.getId());
		assertEquals("50|doi_________::000374d100a9db469bd42b69dbb40b36", d.getId());
		assertEquals(2, d.getOriginalId().size());
		assertTrue(d.getOriginalId().stream().anyMatch(oid -> oid.equals("oai:zenodo.org:3234526")));
		assertValidId(d.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertTrue(d.getAuthor().size() > 0);

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

		assertTrue(d.getSubject().size() > 0);
		assertTrue(d.getInstance().size() > 0);
		assertTrue(d.getContext().size() > 0);
		assertTrue(d.getContext().get(0).getId().length() > 0);

		assertNotNull(d.getInstance());
		assertTrue(d.getInstance().size() > 0);
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
		assertTrue(p.getAuthor().size() > 0);

		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.findFirst();
		assertTrue(author.isPresent());

		assertEquals("Potwarka, Luke R.", author.get().getFullname());
		assertEquals("Potwarka", author.get().getSurname());
		assertEquals("Luke R.", author.get().getName());

		assertTrue(p.getSubject().size() > 0);
		assertTrue(p.getInstance().size() > 0);

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());

		assertNotNull(p.getInstance());
		assertTrue(p.getInstance().size() > 0);
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("UNKNOWN", p.getInstance().get(0).getRefereed().getClassid());
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

		assertEquals("UNKNOWN", i.getRefereed().getClassid());
	}

	@Test
	void testSoftware() throws IOException {
		final String xml = IOUtils.toString(Objects.requireNonNull(getClass().getResourceAsStream("odf_software.xml")));

		final List<Oaf> list = new OdfToOafMapper(vocs, false, true).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Software);

		final Software s = (Software) list.get(0);

		assertValidId(s.getId());
		assertValidId(s.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(s.getTitle().get(0).getValue()));
		assertTrue(s.getAuthor().size() > 0);
		assertTrue(s.getSubject().size() > 0);
		assertTrue(s.getInstance().size() > 0);
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

		final Publication p_cleaned = cleanup(fixVocabularyNames(p));

		assertNotNull(p_cleaned.getTitle());
		assertFalse(p_cleaned.getTitle().isEmpty());
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
		assertTrue(p.getAuthor().size() > 0);

		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.findFirst();
		assertTrue(author.isPresent());

		assertEquals("Museum Sønderjylland", author.get().getFullname());

		assertTrue(p.getSubject().size() > 0);
		assertTrue(p.getInstance().size() > 0);

		assertNotNull(p.getTitle());
		assertFalse(p.getTitle().isEmpty());

		assertNotNull(p.getInstance());
		assertTrue(p.getInstance().size() > 0);
		p
			.getInstance()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("UNKNOWN", i.getAccessright().getClassid());
			});
		assertEquals("UNKNOWN", p.getInstance().get(0).getRefereed().getClassid());
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
