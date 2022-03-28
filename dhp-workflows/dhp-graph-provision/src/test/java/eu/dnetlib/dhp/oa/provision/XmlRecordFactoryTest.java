
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntityWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class XmlRecordFactoryTest {

	public static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testXMLRecordFactory() throws IOException, DocumentException {

		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity<>(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		System.out.println(doc.asXML());

		assertEquals("0000-0001-9613-6638", doc.valueOf("//creator[@rank = '1']/@orcid"));
		assertEquals("0000-0001-9613-6639", doc.valueOf("//creator[@rank = '1']/@orcid_pending"));

		assertEquals("0000-0001-9613-9956", doc.valueOf("//creator[@rank = '2']/@orcid"));
		assertEquals("", doc.valueOf("//creator[@rank = '2']/@orcid_pending"));

		assertEquals("doi", doc.valueOf("//instance/pid/@classid"));
		assertEquals("10.1109/TED.2018.2853550", doc.valueOf("//instance/pid/text()"));

		assertEquals("doi", doc.valueOf("//instance/alternateidentifier/@classid"));
		assertEquals("10.5689/LIB.2018.2853550", doc.valueOf("//instance/alternateidentifier/text()"));

		assertEquals(3, doc.selectNodes("//instance").size());

		assertEquals("1721.47", doc.valueOf("//processingchargeamount/text()"));
		assertEquals("EUR", doc.valueOf("//processingchargecurrency/text()"));

		assertEquals(
			"1.00889953098e-08", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'influence']/@value"));
		assertEquals(
			"30.6576853333", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity_alt']/@value"));
		assertEquals(
			"4.62970429725e-08", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity']/@value"));
	}

	@Test
	public void testXMLRecordFactoryWithValidatedProject() throws IOException, DocumentException {

		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);
		final Project pj = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("project.json")), Project.class);
		final Relation rel = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("relToValidatedProject.json")), Relation.class);
		final RelatedEntity relatedProject = CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class);
		final List<RelatedEntityWrapper> links = Lists.newArrayList();
		final RelatedEntityWrapper rew = new RelatedEntityWrapper(rel, relatedProject);
		links.add(rew);
		final JoinedEntity je = new JoinedEntity<>(p);
		je.setLinks(links);

		final String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));
		assertNotNull(doc);
		System.out.println(doc.asXML());
		assertEquals("2021-01-01", doc.valueOf("//validated/@date"));
	}

	@Test
	public void testXMLRecordFactoryWithNonValidatedProject() throws IOException, DocumentException {

		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);
		final Project pj = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("project.json")), Project.class);
		final Relation rel = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("relToProject.json")), Relation.class);
		final RelatedEntity relatedProject = CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class);
		final List<RelatedEntityWrapper> links = Lists.newArrayList();
		final RelatedEntityWrapper rew = new RelatedEntityWrapper(rel, relatedProject);
		links.add(rew);
		final JoinedEntity je = new JoinedEntity<>(p);
		je.setLinks(links);

		final String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));
		assertNotNull(doc);
		System.out.println(doc.asXML());
		assertEquals("", doc.valueOf("//rel/validated"));
	}

	@Test
	public void testDatasource() throws IOException, DocumentException {
		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Datasource d = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("datasource.json")), Datasource.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity<>(d));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		System.out.println(doc.asXML());

		// TODO add assertions based of values extracted from the XML record

		assertEquals("National", doc.valueOf("//jurisdiction/@classname"));
		assertEquals("true", doc.valueOf("//thematic"));
		assertEquals("Journal article", doc.valueOf("//contentpolicy/@classname"));
		assertEquals("Journal archive", doc.valueOf("//datasourcetypeui/@classname"));

	}
}
