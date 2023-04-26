
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;

public class MergeUtilsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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

		final Result p1d2 = MergeUtils.merge(p1, d2);
		assertEquals(Result.RESULTTYPE.publication, p1d2.getResulttype());
		assertTrue(p1d2 instanceof Publication);
		assertEquals(p1.getId(), p1d2.getId());
	}

	@Test
	void testMergePubs_1() throws IOException {
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);

		final Result p2d1 = MergeUtils.merge(p2, d1);
		assertEquals(Result.RESULTTYPE.dataset, p2d1.getResulttype());
		assertTrue(p2d1 instanceof Dataset);
		assertEquals(d1.getId(), p2d1.getId());
		assertEquals(2, p2d1.getCollectedfrom().size());
	}

	@Test
	void testMergePubs_2() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);

		Result p1p2 = MergeUtils.merge(p1, p2);
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

		Result res = MergeUtils.merge(d1, d2, true);

		assertEquals(d2, res);
	}

	@Test
	void testDelegatedAuthority_2() throws IOException {
		Dataset p1 = read("publication_1.json", Dataset.class);
		Dataset d2 = read("dataset_delegated.json", Dataset.class);

		assertEquals(1, d2.getCollectedfrom().size());
		assertTrue(cfId(d2.getCollectedfrom()).contains(ModelConstants.ZENODO_OD_ID));

		Result res = MergeUtils.merge(p1, d2, true);

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
