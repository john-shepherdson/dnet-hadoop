
package eu.dnetlib.dhp.oa.provision;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;

public class IndexRecordTransformerTest {

	@Test
	public void testTrasformRecord() throws IOException, TransformerException {
		String fields = IOUtils.toString(getClass().getResourceAsStream("fields.xml"));
		String record = IOUtils.toString(getClass().getResourceAsStream("record.xml"));
		String xslt = IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl"));

		String transformer = XmlIndexingJob.getLayoutTransformer("DMF", fields, xslt);

		Transformer tr = SaxonTransformerFactory.newInstance(transformer);

		String a = XmlIndexingJob.toIndexRecord(tr, record);

		System.out.println(a);

	}

}
