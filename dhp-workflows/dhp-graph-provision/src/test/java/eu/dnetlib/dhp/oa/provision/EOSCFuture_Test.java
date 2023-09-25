
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;

public class EOSCFuture_Test {

	public static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static final String VERSION = "2021-04-15T10:05:53Z";
	public static final String DSID = "b9ee796a-c49f-4473-a708-e7d67b84c16d_SW5kZXhEU1Jlc291cmNlcy9JbmRleERTUmVzb3VyY2VUeXBl";

	private ContextMapper contextMapper;

	@BeforeEach
	public void setUp() {
		contextMapper = new ContextMapper();
	}

	@Test
	public void testEOSC_ROHub() throws IOException, DocumentException, TransformerException {

		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final OtherResearchProduct p = OBJECT_MAPPER
			.readValue(
				IOUtils.toString(getClass().getResourceAsStream("eosc-future/photic-zone.json")),
				OtherResearchProduct.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity<>(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);
		System.out.println(doc.asXML());

		testRecordTransformation(xml);
	}

	private void testRecordTransformation(final String record) throws IOException, TransformerException {
		final String fields = IOUtils.toString(getClass().getResourceAsStream("fields.xml"));
		final String xslt = IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl"));

		final String transformer = XmlIndexingJob.getLayoutTransformer("DMF", fields, xslt);

		final Transformer tr = SaxonTransformerFactory.newInstance(transformer);

		final String indexRecordXML = XmlIndexingJob.toIndexRecord(tr, record);

		final SolrInputDocument solrDoc = new StreamingInputDocumentFactory().parseDocument(indexRecordXML);

		final String xmlDoc = ClientUtils.toXML(solrDoc);

		Assertions.assertNotNull(xmlDoc);
		System.out.println(xmlDoc);
	}

}
