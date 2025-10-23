
package eu.dnetlib.dhp.bulktag;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.CommunityConfigurationFactory;
import eu.dnetlib.dhp.bulktag.community.Constraint;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

/** Created by miriam on 03/08/2018. */
class CommunityConfigurationFactoryTest {

	private final VerbResolver resolver = new VerbResolver();

	@Test
	void parseTest() throws DocumentException, IOException, SAXException {
		String xml = IOUtils
			.toString(
				getClass()
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/communityconfiguration/community_configuration.xml"));
		final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
		Assertions.assertEquals(5, cc.size());
		cc
			.getCommunityList()
			.forEach(c -> Assertions.assertTrue(StringUtils.isNoneBlank(c.getId())));
	}

	@Test
	void applyVerb()
		throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
		InstantiationException {
		Constraint sc = new Constraint();
		sc.setVerb("not_contains");
		sc.setField("contributor");
		sc.setValue("DARIAH");
		sc.setSelection(resolver);// .getSelectionCriteria(sc.getVerb(), sc.getValue()));
		String metadata = "This work has been partially supported by DARIAH-EU infrastructure";
		Assertions.assertFalse(sc.verifyCriteria(metadata));
	}

	@Test
	void applyVerbNotContainsPids()
			throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
			InstantiationException {
		Constraint sc = new Constraint();
		sc.setVerb("not_contains");
		sc.setField("pids");
		sc.setValue(Arrays.asList("doi", "pmid", "pmc", "arxiv","handle"));
		sc.setSelection(resolver);// .getSelectionCriteria(sc.getVerb(), sc.getValue()));
		List<String> metadata = List.of("urn");
		Assertions.assertTrue(sc.verifyCriteria(metadata));
	}

	@Test
	void applyVerbExistAny()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
            InstantiationException, JsonProcessingException {
		Constraint sc = new Constraint();

		Field<String> dateOfAcceptance = OafMapperUtils.field("2025-03-30", OafMapperUtils.dataInfo(false, null, false, false, null, "0.9"));
		sc.setVerb("exist");
		sc.setField("publicationDate");
		sc.setValue(null);
		sc.setJsonPath("$.value");
		sc.setSelection(resolver);// .getSelectionCriteria(sc.getVerb(), sc.getValue()));
		Assertions.assertTrue(sc.verifyCriteria(new ObjectMapper().writeValueAsString(dateOfAcceptance)));
	}

	@Test
	void applyVerbExist()
			throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
			InstantiationException, JsonProcessingException {
		Constraint sc = new Constraint();

		Author author = new Author();
		author.setName("Miriam");
		author.setSurname("Baglioni");
		author.setFullname("Baglioni, Miriam");
		author.setPid(new ArrayList<>());
		author.setRawAffiliationString(List.of("ISTI - CNR"));
		author.setRank(1);

		sc.setVerb("exist");
		sc.setField("author");
		sc.setValue(List.of("orcid", "fakepid"));
		sc.setJsonPath("$['pid'][*]['qualifier'].classid");
		sc.setSelection(resolver);// .getSelectionCriteria(sc.getVerb(), sc.getValue()));
		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(author)));
		author.setPid(null);
		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(author)));
		List<StructuredProperty> pids = new ArrayList<>();
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("pid","orcid","orcid","orcid"), null));
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("mag","orcid","orcid","orcid"), null));
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("researcherId","orcid","orcid","orcid"), null));
		author.setPid(pids);
		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(author)));
		pids.add(OafMapperUtils.structuredProperty("orcid", OafMapperUtils.qualifier("orcid","orcid","orcid","orcid"), null));
		author.setPid(pids);
		Assertions.assertTrue(sc.verifyCriteria(new ObjectMapper().writeValueAsString(author)));

		sc.setValue(List.of("orcids", "researcherId"));
		Assertions.assertTrue(sc.verifyCriteria(new ObjectMapper().writeValueAsString(author)));
		pids.add(OafMapperUtils.structuredProperty("orcid", OafMapperUtils.qualifier("orcid","orcid","orcid","orcid"), null));


	}

	@Test
	void applyVerbExistForAll()
			throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
			InstantiationException, JsonProcessingException {
		Constraint sc = new Constraint();

		List<Author> authors = new ArrayList<>();
		Author author = new Author();
		author.setName("Miriam");
		author.setSurname("Baglioni");
		author.setFullname("Baglioni, Miriam");
		author.setPid(new ArrayList<>());
		author.setRawAffiliationString(List.of("ISTI - CNR"));
		author.setRank(1);
		authors.add(author);
		authors.add(author);
		authors.add(author);

		sc.setVerb("exist_forall");
		sc.setField("authors");
		sc.setValue(List.of("orcid", "fakepid"));
		sc.setJsonPath("$['pid'][*]['qualifier'].classid");
		sc.setSelection(resolver);// .getSelectionCriteria(sc.getVerb(), sc.getValue()));
		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(authors)));

		Author author1 = new Author();
		List<StructuredProperty> pids = new ArrayList<>();
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("pid","orcid","orcid","orcid"), null));
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("mag","orcid","orcid","orcid"), null));
		pids.add(OafMapperUtils.structuredProperty("fakePid", OafMapperUtils.qualifier("researcherId","orcid","orcid","orcid"), null));
		author1.setPid(pids);
		authors.add(author1);
		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(authors)));

		pids.add(OafMapperUtils.structuredProperty("orcid", OafMapperUtils.qualifier("orcid","orcid","orcid","orcid"), null));
		author1.setPid(pids);

		Assertions.assertFalse(sc.verifyCriteria(new ObjectMapper().writeValueAsString(authors)));

		author.setPid(pids);
		Assertions.assertTrue(sc.verifyCriteria(new ObjectMapper().writeValueAsString(authors)));
	}

	@Test
	void loadSelCriteriaTest() throws DocumentException, IOException, SAXException {
		String xml = IOUtils
			.toString(
				getClass()
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/communityconfiguration/community_configuration_selcrit.xml"));
		final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
		Map<String, Object> param = new HashMap<>();
		param.put("author", new ArrayList<>(Collections.singletonList("Pippo Pippi")));
		param
			.put(
				"description",
				new ArrayList<>(
					Collections
						.singletonList(
							"This work has been partially supported by DARIAH-EU infrastructure")));
		param
			.put(
				"contributor",
				new ArrayList<>(
					Collections
						.singletonList(
							"Author X helped to write the paper. X works for DARIAH")));
		List<String> comm = cc
			.getCommunityForDatasource(
				"openaire____::1cfdb2e14977f31a98e0118283401f32", param);
		Assertions.assertEquals(1, comm.size());
		Assertions.assertEquals("dariah", comm.get(0));
	}

	@Test
	void loadSelCriteriaTest2() throws DocumentException, IOException, SAXException {
		String xml = IOUtils
			.toString(
				getClass()
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/communityconfiguration/community_configuration_selcrit2.xml"));
		final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
		Map<String, Object> param = new HashMap<>();
		param.put("author", new ArrayList<>(Collections.singletonList("Pippo Pippi")));
		param
			.put(
				"description",
				new ArrayList<>(
					Collections
						.singletonList(
							"This work has been partially supported by DARIAH-EU infrastructure")));
		param
			.put(
				"contributor",
				new ArrayList<>(
					Collections
						.singletonList(
							"Author X helped to write the paper. X works for DARIAH")));
		List<String> comm = cc
			.getCommunityForDatasource(
				"openaire____::1cfdb2e14977f31a98e0118283401f32", param);

		// TODO add more assertions
		Assertions.assertEquals(0, comm.size());
	}

}
