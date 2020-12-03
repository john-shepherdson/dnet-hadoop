
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

//TODO to enable it we need to update the joined_entity.json test file
//@Disabled
public class XmlRecordFactoryTest {

	private static final String otherDsTypeId = "scholarcomminfra,infospace,pubsrepository::mock,entityregistry,entityregistry::projects,entityregistry::repositories,websource";

	@Test
	public void testXMLRecordFactory() throws IOException, DocumentException {

		String json = IOUtils.toString(getClass().getResourceAsStream("joined_entity.json"));

		assertNotNull(json);
		JoinedEntity je = new ObjectMapper().readValue(json, JoinedEntity.class);
		assertNotNull(je);

		Document doc = buildXml(je);
		//// TODO specific test assertion on doc
	}

	@Test
	void testBologna() throws IOException, DocumentException {
		final String json = IOUtils.toString(getClass().getResourceAsStream("oaf-bologna.json"));
		Publication oaf = new ObjectMapper().readValue(json, Publication.class);
		assertNotNull(oaf);
		JoinedEntity je = new JoinedEntity();
		je.setEntity(oaf);
		assertNotNull(je);

		Document doc = buildXml(je);
		// TODO specific test assertion on doc

		System.out.println(doc.asXML());

	}

	private Document buildXml(JoinedEntity je) throws DocumentException {
		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		// TODO add assertions based of values extracted from the XML record

		return doc;
	}
}
