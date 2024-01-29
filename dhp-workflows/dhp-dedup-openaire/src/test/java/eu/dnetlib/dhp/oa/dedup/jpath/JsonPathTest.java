
package eu.dnetlib.dhp.oa.dedup.jpath;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkModel;

class JsonPathTest {

	@Test
	void testJPath() throws IOException {

		DedupConfig conf = DedupConfig
			.load(IOUtils.toString(getClass().getResourceAsStream("dedup_conf_organization.json")));

		final String org = IOUtils.toString(getClass().getResourceAsStream("organization.json"));

		Row row = SparkModel.apply(conf).rowFromJson(org);

		Assertions.assertNotNull(row);
		Assertions.assertTrue(StringUtils.isNotBlank(row.getAs("identifier")));
	}

}
