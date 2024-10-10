
package eu.dnetlib.dhp.oa.dedup.jpath;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import eu.dnetlib.dhp.oa.dedup.SparkOpenorgsDedupTest;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkModel;

class JsonPathTest {

	@Test
	void jsonToModelTest() throws IOException {
		DedupConfig conf = DedupConfig
			.load(
				IOUtils
					.toString(
						SparkOpenorgsDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

		final String org = IOUtils.toString(getClass().getResourceAsStream("organization_example1.json"));

		Row row = SparkModel.apply(conf).rowFromJson(org);
		// to check that the same parsing returns the same row
		Row row1 = SparkModel.apply(conf).rowFromJson(org);

		Assertions.assertEquals(row, row1);
		System.out.println("row = " + row);
		Assertions.assertNotNull(row);
		Assertions.assertTrue(StringUtils.isNotBlank(row.getAs("identifier")));
	}

	@Test
	void testJPath() throws IOException {

		DedupConfig conf = DedupConfig
			.load(IOUtils.toString(getClass().getResourceAsStream("dedup_conf_organization.json")));

		final String org = IOUtils.toString(getClass().getResourceAsStream("organization.json"));

		Row row = SparkModel.apply(conf).rowFromJson(org);

		System.out.println("row = " + row);
		Assertions.assertNotNull(row);
		Assertions.assertTrue(StringUtils.isNotBlank(row.getAs("identifier")));

		System.out.println("row = " + row.getAs("country"));
	}

	@Test
	void testJPath2() throws IOException {

		DedupConfig conf = DedupConfig
			.load(IOUtils.toString(getClass().getResourceAsStream("dedup_conf_dataset.json")));

		final String dat = IOUtils.toString(getClass().getResourceAsStream("dataset_example1.json"));

		Row row = SparkModel.apply(conf).rowFromJson(dat);

		System.out.println("row = " + row);
		Assertions.assertNotNull(row);
		Assertions.assertTrue(StringUtils.isNotBlank(row.getAs("identifier")));

	}
}
