
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.Publication;

public class XmlRecordFactoryTest {

	private static final String otherDsTypeId = "scholarcomminfra,infospace,pubsrepository::mock,entityregistry,entityregistry::projects,entityregistry::repositories,websource";

	@Test
	public void testXMLRecordFactory() throws IOException, DocumentException {

		ContextMapper contextMapper = new ContextMapper();

		XmlRecordFactory xmlRecordFactory = new XmlRecordFactory(contextMapper, false, XmlConverterJob.schemaLocation,
			otherDsTypeId);

		Publication p = new ObjectMapper()
			.readValue(IOUtils.toString(getClass().getResourceAsStream("publication.json")), Publication.class);

		String xml = xmlRecordFactory.build(new JoinedEntity<>(p));

		assertNotNull(xml);

		Document doc = new SAXReader().read(new StringReader(xml));

		assertNotNull(doc);

		// System.out.println(doc.asXML());

		Assertions.assertEquals("0000-0001-9613-6639", doc.valueOf("//creator[@rank = '1']/@orcid"));
		Assertions.assertEquals("0000-0001-9613-6639", doc.valueOf("//creator[@rank = '1']/@orcid_pending"));
		// TODO add assertions based of values extracted from the XML record
	}
}
