
package eu.dnetlib.dhp.oa.graph.raw;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class CopyHdfsOafSparkApplicationTest {

	@Test
	void testIsOafType() throws IOException {
		assertTrue(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/publication_1.json")),
					"publication"));
		assertTrue(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/dataset_1.json")),
					"dataset"));
		assertTrue(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/relation_1.json")),
					"relation"));

		assertFalse(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/publication_1.json")),
					"dataset"));
		assertFalse(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/raw/dataset_1.json")),
					"publication"));

		assertTrue(
			CopyHdfsOafSparkApplication
				.isOafType(
					IOUtils
						.toString(
							getClass()
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/raw/publication_2_unknownProperty.json")),
					"publication"));

	}

}
