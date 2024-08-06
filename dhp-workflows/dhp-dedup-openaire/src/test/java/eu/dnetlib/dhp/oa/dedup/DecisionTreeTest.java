
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import eu.dnetlib.dhp.oa.dedup.SparkOpenorgsDedupTest;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkModel;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import eu.dnetlib.pace.tree.support.TreeStats;

class DecisionTreeTest {

	@Test
	void testJPath() throws IOException {

		DedupConfig conf = DedupConfig
			.load(IOUtils.toString(getClass().getResourceAsStream("dedup_conf_organization.json")));

		final String org = IOUtils.toString(getClass().getResourceAsStream("organization.json"));

		Row row = SparkModel.apply(conf).rowFromJson(org);

		System.out.println("row = " + row);
		Assertions.assertNotNull(row);
		Assertions.assertTrue(StringUtils.isNotBlank(row.getAs("identifier")));

		System.out.println("row = " + row.getAs("countrytitle"));
	}

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
	void organizationDecisionTreeTest() throws Exception {
		DedupConfig conf = DedupConfig
			.load(
				IOUtils
					.toString(
						SparkOpenorgsDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

		final String org1 = "{\"eclegalbody\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecresearchorganization\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"legalname\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"SUSF - Universit\\\\u00e9 internationale de floride\"}, \"pid\": [{\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"qualifier\": {\"classid\": \"grid\", \"classname\": \"grid\", \"schemename\": \"dnet:pid_types\", \"schemeid\": \"dnet:pid_types\"}, \"value\": \"grid.65456.34\"}], \"websiteurl\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"http://www.fiu.edu/\"}, \"ecnutscode\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"logourl\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"collectedfrom\": [{\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"GRID - Global Research Identifier Database\", \"key\": \"10|openaire____::ff4a008470319a22d9cf3d14af485977\"}], \"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"UNKNOWN\", \"classname\": \"UNKNOWN\", \"schemename\": \"dnet:provenanceActions\", \"schemeid\": \"dnet:provenanceActions\"}, \"inferred\": true, \"inferenceprovenance\": \"dedup-similarity-organization-simple\", \"invisible\": false, \"trust\": \"0.89\"}, \"alternativeNames\": [], \"echighereducation\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"id\": \"20|grid________::f22e08fb7bd544b4355f99bef2c43ad5\", \"eclegalperson\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"lastupdatetimestamp\": 1566902405602, \"ecinternationalorganizationeurinterests\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"dateofcollection\": \"\", \"dateoftransformation\": \"\", \"ecnonprofit\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecenterprise\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecinternationalorganization\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"legalshortname\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"FIU\"}, \"country\": {\"classid\": \"US\", \"classname\": \"United States\", \"schemename\": \"dnet:countries\", \"schemeid\": \"dnet:countries\"}, \"extraInfo\": [], \"originalId\": [], \"ecsmevalidated\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}}";
		final String org2 = "{\"eclegalbody\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecresearchorganization\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"legalname\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"SUSF - Universidad Internacional de Florida\"}, \"pid\": [{\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"qualifier\": {\"classid\": \"grid\", \"classname\": \"grid\", \"schemename\": \"dnet:pid_types\", \"schemeid\": \"dnet:pid_types\"}, \"value\": \"grid.65456.34\"}], \"websiteurl\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"http://www.fiu.edu/\"}, \"ecnutscode\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"logourl\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"collectedfrom\": [{\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"GRID - Global Research Identifier Database\", \"key\": \"10|openaire____::ff4a008470319a22d9cf3d14af485977\"}], \"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"UNKNOWN\", \"classname\": \"UNKNOWN\", \"schemename\": \"dnet:provenanceActions\", \"schemeid\": \"dnet:provenanceActions\"}, \"inferred\": true, \"inferenceprovenance\": \"dedup-similarity-organization-simple\", \"invisible\": false, \"trust\": \"0.89\"}, \"alternativeNames\": [], \"echighereducation\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"id\": \"20|grid________::2b261e9d8c2a63abbfd5826918c23b6d\", \"eclegalperson\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"lastupdatetimestamp\": 1566902405602, \"ecinternationalorganizationeurinterests\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"dateofcollection\": \"\", \"dateoftransformation\": \"\", \"ecnonprofit\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecenterprise\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"ecinternationalorganization\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}, \"legalshortname\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"FIU\"}, \"country\": {\"classid\": \"US\", \"classname\": \"United States\", \"schemename\": \"dnet:countries\", \"schemeid\": \"dnet:countries\"}, \"extraInfo\": [], \"originalId\": [], \"ecsmevalidated\": {\"dataInfo\": {\"deletedbyinference\": false, \"provenanceaction\": {\"classid\": \"\", \"classname\": \"\", \"schemename\": \"\", \"schemeid\": \"\"}, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"\"}, \"value\": \"\"}}";

		Row row1 = SparkModel.apply(conf).rowFromJson(org1);
		Row row2 = SparkModel.apply(conf).rowFromJson(org2);

		System.out.println("row1 = " + row1);
		System.out.println("row2 = " + row2);
		TreeProcessor tree = new TreeProcessor(conf);

		boolean result = tree.compare(row1, row2);

		System.out.println("result = " + result);

	}

}