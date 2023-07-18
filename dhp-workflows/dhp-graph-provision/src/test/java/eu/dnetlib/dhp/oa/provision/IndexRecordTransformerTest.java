
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntityWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;

/**
 * This test can be used to produce a record that can be manually fed to Solr in XML format.
 *
 * The input is a JoinedEntity, i.e. a json representation of an OpenAIRE entity that embeds all the linked entities.
 */
public class IndexRecordTransformerTest {

	public static final String VERSION = "2021-04-15T10:05:53Z";
	public static final String DSID = "b9ee796a-c49f-4473-a708-e7d67b84c16d_SW5kZXhEU1Jlc291cmNlcy9JbmRleERTUmVzb3VyY2VUeXBl";

	private ContextMapper contextMapper;

	@BeforeEach
	public void setUp() {
		contextMapper = new ContextMapper();
	}

	@Test
	public void testPreBuiltRecordTransformation() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("record.xml"));

		testRecordTransformation(record);
	}

	@Test
	public void testPublicationRecordTransformation() throws IOException, TransformerException {

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = load("publication.json", Publication.class);
		final Project pj = load("project.json", Project.class);
		final Relation rel = load("relToValidatedProject.json", Relation.class);

		final JoinedEntity je = new JoinedEntity<>(p);
		je
			.setLinks(
				Lists
					.newArrayList(
						new RelatedEntityWrapper(rel,
							CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class))));

		final String record = xmlRecordFactory.build(je);

		assertNotNull(record);

		testRecordTransformation(record);
	}

	@Test
	public void testRiunet() throws IOException, TransformerException {

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = load("riunet.json", Publication.class);

		final JoinedEntity je = new JoinedEntity<>(p);
		final String record = xmlRecordFactory.build(je);
		assertNotNull(record);
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureDataTransferPilot() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("eosc-future/data-transfer-pilot.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureTraining() throws IOException, TransformerException {
		final String record = IOUtils
			.toString(getClass().getResourceAsStream("eosc-future/training-notebooks-seadatanet.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureAirQualityCopernicus() throws IOException, TransformerException {
		final String record = IOUtils
			.toString(getClass().getResourceAsStream("eosc-future/air-quality-copernicus.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureB2SharePlotSw() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("eosc-future/b2share-plot-sw.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureB2SharePlotRelatedORP() throws IOException, TransformerException {
		final String record = IOUtils
			.toString(getClass().getResourceAsStream("eosc-future/b2share-plot-related-orp.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEOSCFutureSentinel() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("eosc-future/sentinel.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEdithDemo() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("edith-demo/10.1098-rsta.2020.0257.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEdithDemoCovid() throws IOException, TransformerException {
		final String record = IOUtils
			.toString(getClass().getResourceAsStream("edith-demo/10.3390-pr9111967-covid.xml"));
		testRecordTransformation(record);
	}

	@Test
	public void testForEdithDemoEthics() throws IOException, TransformerException {
		final String record = IOUtils.toString(getClass().getResourceAsStream("edith-demo/10.2196-33081-ethics.xml"));
		testRecordTransformation(record);
	}

	@Test
	void testDoiUrlNormalization() throws MalformedURLException {

		// TODO add more test examples when needed
		List<String> urls = Arrays
			.asList(
				"https://dx.doi.org/10.1016/j.jas.2019.105013",
				"http://dx.doi.org/10.13140/rg.2.2.26964.65927",
				"https://dx.doi.org/10.13140/rg.2.2.26964.65927",
				"http://dx.doi.org/10.1016/j.jas.2019.105013",
				"http://hdl.handle.net/2072/369223",
				"https://doi.org/10.1016/j.jas.2019.105013");

		for (String url : urls) {
			URL u = new URL(XmlRecordFactory.normalizeDoiUrl(url));
			if (url.contains(XmlRecordFactory.DOI_ORG_AUTHORITY)) {
				assertEquals(XmlRecordFactory.HTTPS, u.getProtocol());
				assertEquals(XmlRecordFactory.DOI_ORG_AUTHORITY, u.getAuthority());
			} else {
				assertEquals(url, u.toString());
			}
		}
	}

	private void testRecordTransformation(final String record) throws IOException, TransformerException {
		final String fields = IOUtils.toString(getClass().getResourceAsStream("fields.xml"));
		final String xslt = IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl"));

		final String transformer = XmlIndexingJob.getLayoutTransformer("DMF", fields, xslt);

		final Transformer tr = SaxonTransformerFactory.newInstance(transformer);

		final String indexRecordXML = XmlIndexingJob.toIndexRecord(tr, record);

		final SolrInputDocument solrDoc = new StreamingInputDocumentFactory(VERSION, DSID)
			.parseDocument(indexRecordXML);

		final String xmlDoc = ClientUtils.toXML(solrDoc);

		Assertions.assertNotNull(xmlDoc);
		System.out.println(xmlDoc);
	}

	private <T> T load(final String fileName, final Class<T> clazz) throws IOException {
		return XmlRecordFactoryTest.OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream(fileName)), clazz);
	}

}
