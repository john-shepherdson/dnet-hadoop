
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;

public class XmlRecordFactoryTest {

	private static final String otherDsTypeId = "scholarcomminfra,infospace,pubsrepository::mock,entityregistry,entityregistry::projects,entityregistry::repositories,websource";

	@Test
	public void testXMLRecordFactory() throws IOException, DocumentException {

		String json = IOUtils.toString(getClass().getResourceAsStream("joined_entity.json"));

		assertNotNull(json);
		JoinedEntity je = new ObjectMapper().readValue(json, JoinedEntity.class);
		assertNotNull(je);

		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		String xml = xmlRecordFactory.build(je);

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		System.out.println(doc.asXML());

	}
}
