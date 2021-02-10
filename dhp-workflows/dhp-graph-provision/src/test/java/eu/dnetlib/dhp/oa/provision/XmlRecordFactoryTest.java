
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntityWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class XmlRecordFactoryTest {

	private static final String otherDsTypeId = "scholarcomminfra,infospace,pubsrepository::mock,entityregistry,entityregistry::projects,entityregistry::repositories,websource";

	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testXMLRecordFactory() throws IOException, DocumentException {

		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);

		String xml = xmlRecordFactory.build(new JoinedEntity<>(p));

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		System.out.println(doc.asXML());

		Assertions.assertEquals("0000-0001-9613-6638", doc.valueOf("//creator[@rank = '1']/@orcid"));
		Assertions.assertEquals("0000-0001-9613-6639", doc.valueOf("//creator[@rank = '1']/@orcid_pending"));

		Assertions.assertEquals("0000-0001-9613-9956", doc.valueOf("//creator[@rank = '2']/@orcid"));
		Assertions.assertEquals("", doc.valueOf("//creator[@rank = '2']/@orcid_pending"));

		// TODO add assertions based of values extracted from the XML record
	}

	@Test
	public void testXMLRecordFactoryWithValidatedProject() throws IOException, DocumentException {

		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);
		Project pj = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("project.json")), Project.class);
		Relation rel = OBJECT_MAPPER
			.readValue(
				(IOUtils.toString(getClass().getResourceAsStream("relToValidatedProject.json"))), Relation.class);
		RelatedEntity relatedProject = CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class);
		List<RelatedEntityWrapper> links = Lists.newArrayList();
		RelatedEntityWrapper rew = new RelatedEntityWrapper(rel, relatedProject);
		links.add(rew);
		JoinedEntity je = new JoinedEntity<>(p);
		je.setLinks(links);

		String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));
		assertNotNull(doc);
		System.out.println(doc.asXML());
		Assertions.assertEquals("2021-01-01", doc.valueOf("//validated/@date"));
	}

	@Test
	public void testXMLRecordFactoryWithNonValidatedProject() throws IOException, DocumentException {

		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);
		Project pj = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("project.json")), Project.class);
		Relation rel = OBJECT_MAPPER
			.readValue((IOUtils.toString(getClass().getResourceAsStream("relToProject.json"))), Relation.class);
		RelatedEntity relatedProject = CreateRelatedEntitiesJob_phase1.asRelatedEntity(pj, Project.class);
		List<RelatedEntityWrapper> links = Lists.newArrayList();
		RelatedEntityWrapper rew = new RelatedEntityWrapper(rel, relatedProject);
		links.add(rew);
		JoinedEntity je = new JoinedEntity<>(p);
		je.setLinks(links);

		String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));
		assertNotNull(doc);
		System.out.println(doc.asXML());
		assertEquals("", doc.valueOf("//rel/validated"));
	}
}
