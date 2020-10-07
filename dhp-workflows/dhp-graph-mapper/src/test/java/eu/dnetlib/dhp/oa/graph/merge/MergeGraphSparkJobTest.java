
package eu.dnetlib.dhp.oa.graph.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Datasource;

public class MergeGraphSparkJobTest {

	private ObjectMapper mapper;

	@BeforeEach
	public void setUp() {
		mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Test
	public void testMergeDatasources() throws IOException {
		assertEquals(
			"openaire-cris_1.1",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_cris.json"),
					d("datasource_UNKNOWN.json"))
				.getOpenairecompatibility()
				.getClassid());
		assertEquals(
			"openaire-cris_1.1",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_UNKNOWN.json"),
					d("datasource_cris.json"))
				.getOpenairecompatibility()
				.getClassid());
		assertEquals(
			"driver-openaire2.0",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_native.json"),
					d("datasource_driver-openaire2.0.json"))
				.getOpenairecompatibility()
				.getClassid());
		assertEquals(
			"driver-openaire2.0",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_driver-openaire2.0.json"),
					d("datasource_native.json"))
				.getOpenairecompatibility()
				.getClassid());
		assertEquals(
			"openaire4.0",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_notCompatible.json"),
					d("datasource_openaire4.0.json"))
				.getOpenairecompatibility()
				.getClassid());
		assertEquals(
			"notCompatible",
			MergeGraphSparkJob
				.mergeDatasource(
					d("datasource_notCompatible.json"),
					d("datasource_UNKNOWN.json"))
				.getOpenairecompatibility()
				.getClassid());
	}

	private Optional<Datasource> d(String file) throws IOException {
		String json = IOUtils.toString(getClass().getResourceAsStream(file));
		return Optional.of(mapper.readValue(json, Datasource.class));
	}

}
