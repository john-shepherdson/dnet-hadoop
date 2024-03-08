
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
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
import eu.dnetlib.dhp.schema.oaf.*;

public class XmlRecordFactoryTest {

	public static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	void testXMLRecordFactory() throws IOException, DocumentException {

		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));
		doc.normalize();

		assertNotNull(doc);

		// System.out.println(doc.asXML());

		assertEquals("0000-0001-9613-6638", doc.valueOf("//creator[@rank = '1']/@orcid"));
		assertEquals("0000-0001-9613-6639", doc.valueOf("//creator[@rank = '1']/@orcid_pending"));

		assertEquals("0000-0001-9613-9956", doc.valueOf("//creator[@rank = '2']/@orcid"));
		assertEquals("", doc.valueOf("//creator[@rank = '2']/@orcid_pending"));

		assertEquals("doi", doc.valueOf("//instance/pid/@classid"));
		assertEquals("10.1109/TED.2018.2853550", doc.valueOf("//instance/pid/text()"));

		assertEquals("doi", doc.valueOf("//instance/alternateidentifier/@classid"));
		assertEquals("10.5689/LIB.2018.2853550", doc.valueOf("//instance/alternateidentifier/text()"));

		assertEquals(2, doc.selectNodes("//instance").size());

		assertEquals("1721.47", doc.valueOf("//processingchargeamount/text()"));
		assertEquals("EUR", doc.valueOf("//processingchargecurrency/text()"));

		assertEquals(
			"5.06690394631e-09", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'influence']/@score"));
		assertEquals(
			"C", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'influence']/@class"));

		assertEquals(
			"0.0", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity_alt']/@score"));
		assertEquals(
			"C", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity_alt']/@class"));

		assertEquals(
			"3.11855618382e-09", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity']/@score"));
		assertEquals(
			"C", doc.valueOf("//*[local-name() = 'result']/measure[./@id = 'popularity']/@class"));

		assertEquals("EOSC::Jupyter Notebook", doc.valueOf("//*[local-name() = 'result']/eoscifguidelines/@code"));

		assertEquals(2, Integer.parseInt(doc.valueOf("count(//*[local-name() = 'result']/fulltext)")));

		assertEquals(
			"https://osf.io/preprints/socarxiv/7vgtu/download",
			doc.valueOf("//*[local-name() = 'result']/fulltext[1]"));

		assertEquals("true", doc.valueOf("//*[local-name() = 'result']/isgreen/text()"));
		assertEquals("bronze", doc.valueOf("//*[local-name() = 'result']/openaccesscolor/text()"));
		assertEquals("true", doc.valueOf("//*[local-name() = 'result']/isindiamondjournal/text()"));
		assertEquals("true", doc.valueOf("//*[local-name() = 'result']/publiclyfunded/text()"));
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
		final JoinedEntity je = new JoinedEntity(p);
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
		final JoinedEntity je = new JoinedEntity(p);
		je.setLinks(links);

		final String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));
		assertNotNull(doc);
		System.out.println(doc.asXML());
		assertEquals("", doc.valueOf("//rel/validated"));
	}

	@Test
	public void testService() throws IOException, DocumentException {
		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Datasource d = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResourceAsStream("datasource.json")), Datasource.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity(d));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		System.out.println(doc.asXML());

		// TODO add assertions based of values extracted from the XML record

		assertEquals("National", doc.valueOf("//jurisdiction/@classname"));
		assertEquals("true", doc.valueOf("//thematic"));
		assertEquals("Journal article", doc.valueOf("//contentpolicy/@classname"));
		assertEquals("Journal archive", doc.valueOf("//datasourcetypeui/@classname"));
		assertEquals("Data Source", doc.valueOf("//eosctype/@classname"));

		final List pids = doc.selectNodes("//pid");
		assertEquals(1, pids.size());
		assertEquals("re3data", ((Element) pids.get(0)).attribute("classid").getValue());
		assertEquals(
			"Registry of research data repositories", ((Element) pids.get(0)).attribute("classname").getValue());
		assertEquals("dnet:pid_types", ((Element) pids.get(0)).attribute("schemeid").getValue());
		assertEquals("dnet:pid_types", ((Element) pids.get(0)).attribute("schemename").getValue());
	}

	@Test
	public void testD4ScienceTraining() throws DocumentException, IOException {
		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final OtherResearchProduct p = OBJECT_MAPPER
			.readValue(
				IOUtils.toString(getClass().getResourceAsStream("d4science-1-training.json")),
				OtherResearchProduct.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);
		System.out.println(doc.asXML());

	}

	@Test
	public void testD4ScienceDataset() throws DocumentException, IOException {
		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final OtherResearchProduct p = OBJECT_MAPPER
			.readValue(
				IOUtils.toString(getClass().getResourceAsStream("d4science-2-dataset.json")),
				OtherResearchProduct.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);
		System.out.println(doc.asXML());

	}

	@Test
	public void testIrisGuidelines4() throws DocumentException, IOException {
		final ContextMapper contextMapper = new ContextMapper();

		final XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false,
			XmlConverterJob.schemaLocation);

		final Publication p = OBJECT_MAPPER
			.readValue(
				IOUtils.toString(getClass().getResourceAsStream("iris-odf-4.json")),
				Publication.class);

		final String xml = xmlRecordFactory.build(new JoinedEntity(p));

		assertNotNull(xml);

		final Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);
		System.out.println(doc.asXML());

	}

}
