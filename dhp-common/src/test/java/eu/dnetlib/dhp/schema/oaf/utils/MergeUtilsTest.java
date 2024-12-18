
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class MergeUtilsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	void testMergePubs_new() throws IOException {
		Publication pt = read("publication_test.json", Publication.class);
		Publication p1 = read("publication_test.json", Publication.class);

		assertEquals(1, pt.getCollectedfrom().size());
		assertEquals(ModelConstants.CROSSREF_ID, pt.getCollectedfrom().get(0).getKey());

		Instance i = new Instance();
		i.setUrl(Lists.newArrayList("https://..."));
		p1.getInstance().add(i);

		Publication ptp1 = MergeUtils.mergePublication(pt, p1);

		assertNotNull(ptp1.getInstance());
		assertEquals(2, ptp1.getInstance().size());

	}

	@Test
	void testMergePubs() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);
		Dataset d2 = read("dataset_2.json", Dataset.class);

		assertEquals(1, p1.getCollectedfrom().size());
		assertEquals(ModelConstants.CROSSREF_ID, p1.getCollectedfrom().get(0).getKey());
		assertEquals(1, d2.getCollectedfrom().size());
		assertFalse(cfId(d2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertEquals(1, p2.getCollectedfrom().size());
		assertFalse(cfId(p2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));
		assertEquals(1, d1.getCollectedfrom().size());
		assertTrue(cfId(d1.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		final Result p1d2 = MergeUtils.checkedMerge(p1, d2, true);
		assertEquals(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID, p1d2.getResulttype().getClassid());
		assertTrue(p1d2 instanceof Publication);
		assertEquals(p1.getId(), p1d2.getId());
	}

	@Test
	void testMergePubs_1() throws IOException {
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);

		final Result p2d1 = MergeUtils.checkedMerge(p2, d1, true);
		assertEquals((ModelConstants.DATASET_RESULTTYPE_CLASSID), p2d1.getResulttype().getClassid());
		assertTrue(p2d1 instanceof Dataset);
		assertEquals(d1.getId(), p2d1.getId());
		assertEquals(2, p2d1.getCollectedfrom().size());
	}

	@Test
	void testMergePubs_2() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);

		Result p1p2 = MergeUtils.checkedMerge(p1, p2, true);
		assertTrue(p1p2 instanceof Publication);
		assertEquals(p1.getId(), p1p2.getId());
		assertEquals(2, p1p2.getCollectedfrom().size());
	}

	@Test
	void testDelegatedAuthority_1() throws IOException {
		Dataset d1 = read("dataset_2.json", Dataset.class);
		Dataset d2 = read("dataset_delegated.json", Dataset.class);

		assertEquals(1, d2.getCollectedfrom().size());
		assertTrue(cfId(d2.getCollectedfrom()).contains(ModelConstants.ZENODO_OD_ID));

		Result res = (Result) MergeUtils.merge(d1, d2, true);

		assertEquals(d2, res);
	}

	@Test
	void testDelegatedAuthority_2() throws IOException {
		Dataset p1 = read("publication_1.json", Dataset.class);
		Dataset d2 = read("dataset_delegated.json", Dataset.class);

		assertEquals(1, d2.getCollectedfrom().size());
		assertTrue(cfId(d2.getCollectedfrom()).contains(ModelConstants.ZENODO_OD_ID));

		Result res = (Result) MergeUtils.merge(p1, d2, true);

		assertEquals(d2, res);
	}

	protected HashSet<String> cfId(List<KeyValue> collectedfrom) {
		return collectedfrom.stream().map(KeyValue::getKey).collect(Collectors.toCollection(HashSet::new));
	}

	protected <T extends Result> T read(String filename, Class<T> clazz) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		return OBJECT_MAPPER.readValue(json, clazz);
	}

}
