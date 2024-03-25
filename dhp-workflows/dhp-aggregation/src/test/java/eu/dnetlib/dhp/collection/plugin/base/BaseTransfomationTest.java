
package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest;
import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

// @Disabled
@ExtendWith(MockitoExtension.class)
public class BaseTransfomationTest extends AbstractVocabularyTest {

	private SparkConf sparkConf;

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {
		setUpVocabulary();

		this.sparkConf = new SparkConf();
		this.sparkConf.setMaster("local[*]");
		this.sparkConf.set("spark.driver.host", "localhost");
		this.sparkConf.set("spark.ui.enabled", "false");
	}

	@Test
	void testBase2ODF() throws Exception {

		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("record.xml")));

		final XSLTTransformationFunction tr = loadTransformationRule("xml/base2odf.transformationRule.xml");

		final MetadataRecord result = tr.call(mr);

		System.out.println(result.getBody());
	}

	@Test
	void testBase2OAF() throws Exception {

		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("record.xml")));

		final XSLTTransformationFunction tr = loadTransformationRule("xml/base2oaf.transformationRule.xml");

		final MetadataRecord result = tr.call(mr);

		System.out.println(result.getBody());
	}

	@Test
	void testBase2ODF_wrong_date() throws Exception {

		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("record_wrong_1.xml")));

		final XSLTTransformationFunction tr = loadTransformationRule("xml/base2oaf.transformationRule.xml");

		assertThrows(NullPointerException.class, () -> {
			final MetadataRecord result = tr.call(mr);
			System.out.println(result.getBody());
		});
	}

	private XSLTTransformationFunction loadTransformationRule(final String path) throws Exception {
		final String xslt = new SAXReader()
				.read(this.getClass().getResourceAsStream(path))
				.selectSingleNode("//CODE/*")
				.asXML();

		final LongAccumulator la = new LongAccumulator();

		return new XSLTTransformationFunction(new AggregationCounter(la, la, la), xslt, 0, this.vocabularies);
	}

}
