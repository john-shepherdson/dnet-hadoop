
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

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
