
package eu.dnetlib.dhp.bulktag;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import com.google.gson.Gson;

import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.CommunityConfigurationFactory;
import eu.dnetlib.dhp.bulktag.community.Constraint;
import eu.dnetlib.dhp.bulktag.community.SelectionConstraints;
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
		sc.setSelection(resolver);//.getSelectionCriteria(sc.getVerb(), sc.getValue()));
		String metadata = "This work has been partially supported by DARIAH-EU infrastructure";
		Assertions.assertFalse(sc.verifyCriteria(metadata));
	}

	@Test
	void loadSelCriteriaTest() throws DocumentException, IOException, SAXException {
		String xml = IOUtils
			.toString(
				getClass()
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/communityconfiguration/community_configuration_selcrit.xml"));
		final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
		Map<String, List<String>> param = new HashMap<>();
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
		Map<String, List<String>> param = new HashMap<>();
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
