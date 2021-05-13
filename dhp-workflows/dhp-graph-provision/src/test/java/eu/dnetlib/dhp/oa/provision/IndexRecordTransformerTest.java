
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
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
		String record = IOUtils.toString(getClass().getResourceAsStream("record.xml"));

		testRecordTransformation(record);
	}

	@Test
	public void testPublicationRecordTransformation() throws IOException, TransformerException {

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			XmlRecordFactoryTest.otherDsTypeId);

		Publication p = load("publication.json", Publication.class);
		Project pj = load("project.json", Project.class);
		Relation rel = load("relToValidatedProject.json", Relation.class);

		JoinedEntity je = new JoinedEntity<>(p);
		je
			.setLinks(
				Lists
					.newArrayList(
						new RelatedEntityWrapper(rel,
							CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class))));

		String record = xmlRecordFactory.build(je);

		assertNotNull(record);

		testRecordTransformation(record);
	}

	private void testRecordTransformation(String record) throws IOException, TransformerException {
		String fields = IOUtils.toString(getClass().getResourceAsStream("fields.xml"));
		String xslt = IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl"));

		String transformer = XmlIndexingJob.getLayoutTransformer("DMF", fields, xslt);

		Transformer tr = SaxonTransformerFactory.newInstance(transformer);

		String indexRecordXML = XmlIndexingJob.toIndexRecord(tr, record);

		SolrInputDocument solrDoc = new StreamingInputDocumentFactory(VERSION, DSID).parseDocument(indexRecordXML);

		final String xmlDoc = ClientUtils.toXML(solrDoc);

		Assertions.assertNotNull(xmlDoc);
		System.out.println(xmlDoc);
	}

	private <T> T load(String fileName, Class<T> clazz) throws IOException {
		return XmlRecordFactoryTest.OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream(fileName)), clazz);
	}

}
