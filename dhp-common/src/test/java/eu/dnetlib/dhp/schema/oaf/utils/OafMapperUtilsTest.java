
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

public class OafMapperUtilsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testDateValidation() {

		assertTrue(GraphCleaningFunctions.isValidDate("2016-05-07T12:41:19.202Z"));
		assertTrue(GraphCleaningFunctions.isValidDate("2020-09-10 11:08:52"));
		assertTrue(GraphCleaningFunctions.isValidDate("2016-04-05"));
		assertFalse(GraphCleaningFunctions.isValidDate("2016 April 05"));

	}

	@Test
	public void testMergePubs() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);
		Dataset d2 = read("dataset_2.json", Dataset.class);

		assertEquals(p1.getCollectedfrom().size(), 1);
		assertEquals(p1.getCollectedfrom().get(0).getKey(), ModelConstants.CROSSREF_ID);
		assertEquals(d2.getCollectedfrom().size(), 1);
		assertFalse(cfId(d2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertTrue(
			OafMapperUtils
				.mergeResults(p1, d2)
				.getResulttype()
				.getClassid()
				.equals(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID));

		assertEquals(p2.getCollectedfrom().size(), 1);
		assertFalse(cfId(p2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));
		assertEquals(d1.getCollectedfrom().size(), 1);
		assertTrue(cfId(d1.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertTrue(
			OafMapperUtils
				.mergeResults(p2, d1)
				.getResulttype()
				.getClassid()
				.equals(ModelConstants.DATASET_RESULTTYPE_CLASSID));
	}

	protected HashSet<String> cfId(List<KeyValue> collectedfrom) {
		return collectedfrom.stream().map(c -> c.getKey()).collect(Collectors.toCollection(HashSet::new));
	}

	protected <T extends Result> T read(String filename, Class<T> clazz) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		return OBJECT_MAPPER.readValue(json, clazz);
	}

}
