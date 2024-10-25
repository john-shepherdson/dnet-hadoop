
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.oa.graph.raw.CopyHdfsOafSparkApplication.getOafType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class CopyHdfsOafSparkApplicationTest {

	String getResourceAsStream(String path) throws IOException {
		return IOUtils.toString(getClass().getResourceAsStream(path));
	}

	@Test
	void testIsOafType() throws IOException {
		assertEquals("publication", getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/publication_1.json")));
		assertEquals("dataset", getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/dataset_1.json")));
		assertEquals("relation", getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/relation_1.json")));
		assertEquals("publication", getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/publication_1.json")));
		assertEquals(
			"publication",
			getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/publication_2_unknownProperty.json")));
	}

	@Test
	void isOafType_Datacite_ORP() throws IOException {
		assertEquals(
			"otherresearchproduct", getOafType(getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/datacite_orp.json")));
	}
}
