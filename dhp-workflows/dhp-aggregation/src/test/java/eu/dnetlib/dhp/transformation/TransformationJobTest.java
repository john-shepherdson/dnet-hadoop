
package eu.dnetlib.dhp.transformation;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.functions.Cleaner;
import eu.dnetlib.dhp.transformation.vocabulary.Vocabulary;
import eu.dnetlib.dhp.transformation.vocabulary.VocabularyHelper;
import eu.dnetlib.dhp.utils.DHPUtils;
import net.sf.saxon.s9api.*;

@ExtendWith(MockitoExtension.class)
public class TransformationJobTest {

	@Mock
	private LongAccumulator accumulator;

	@Test
	public void testTransformSaxonHE() throws Exception {

		Map<String, Vocabulary> vocabularies = new HashMap<>();
		vocabularies.put("dnet:languages", VocabularyHelper.getVocabularyFromAPI("dnet:languages"));
		Cleaner cleanFunction = new Cleaner(vocabularies);
		Processor proc = new Processor(false);
		proc.registerExtensionFunction(cleanFunction);
		final XsltCompiler comp = proc.newXsltCompiler();
		XsltExecutable exp = comp
			.compile(
				new StreamSource(
					this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/ext_simple.xsl")));
		XdmNode source = proc
			.newDocumentBuilder()
			.build(
				new StreamSource(
					this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input.xml")));
		XsltTransformer trans = exp.load();
		trans.setInitialContextNode(source);
		final StringWriter output = new StringWriter();
		Serializer out = proc.newSerializer(output);
		out.setOutputProperty(Serializer.Property.METHOD, "xml");
		out.setOutputProperty(Serializer.Property.INDENT, "yes");
		trans.setDestination(out);
		trans.transform();
		System.out.println(output.toString());
	}

	@DisplayName("Test TransformSparkJobNode.main")
	@Test
	public void transformTest(@TempDir Path testDir) throws Exception {
		final String mdstore_input = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstorenative").getFile();
		final String mdstore_output = testDir.toString() + "/version";
		final String xslt = DHPUtils
			.compressString(
				IOUtils
					.toString(
						this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml")));
		TransformSparkJobNode
			.main(
				new String[] {
					"-mt",
					"local",
					"-i",
					mdstore_input,
					"-o",
					mdstore_output,
					"-d",
					"1",
					"-w",
					"1",
					"-tr",
					xslt,
					"-t",
					"true",
					"-ru",
					"",
					"-rp",
					"",
					"-rh",
					"",
					"-ro",
					"",
					"-rr",
					""
				});
	}

	@Test
	public void tryLoadFolderOnCP() throws Exception {
		final String path = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstorenative").getFile();
		System.out.println("path = " + path);

		Path tempDirWithPrefix = Files.createTempDirectory("mdstore_output");

		System.out.println(tempDirWithPrefix.toFile().getAbsolutePath());

		Files.deleteIfExists(tempDirWithPrefix);
	}

	@Test
	public void testTransformFunction() throws Exception {
		SAXReader reader = new SAXReader();
		Document document = reader.read(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml"));
		Node node = document.selectSingleNode("//CODE/*[local-name()='stylesheet']");
		final String xslt = node.asXML();
		Map<String, Vocabulary> vocabularies = new HashMap<>();
		vocabularies.put("dnet:languages", VocabularyHelper.getVocabularyFromAPI("dnet:languages"));

		TransformFunction tf = new TransformFunction(accumulator, accumulator, accumulator, xslt, 1, vocabularies);

		MetadataRecord record = new MetadataRecord();
		record
			.setBody(
				IOUtils
					.toString(
						this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input.xml")));

		final MetadataRecord result = tf.call(record);
		assertNotNull(result.getBody());

		System.out.println(result.getBody());
	}

	@Test
	public void extractTr() throws Exception {

		final String xmlTr = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml"));

		SAXReader reader = new SAXReader();
		Document document = reader.read(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml"));
		Node node = document.selectSingleNode("//CODE/*[local-name()='stylesheet']");

		System.out.println(node.asXML());
	}
}
