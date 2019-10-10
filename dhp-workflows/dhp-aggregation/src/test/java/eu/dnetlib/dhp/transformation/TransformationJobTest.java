package eu.dnetlib.dhp.transformation;

import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.functions.Cleaner;
import eu.dnetlib.dhp.utils.DHPUtils;
import net.sf.saxon.s9api.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;


public class TransformationJobTest {
    @Mock
    LongAccumulator accumulator;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Test
    public void testTransformSaxonHE() throws Exception {
        Cleaner cleanFunction = new Cleaner();
        Processor proc = new Processor(false);
        proc.registerExtensionFunction(cleanFunction);
        final XsltCompiler comp = proc.newXsltCompiler();
        XsltExecutable exp = comp.compile(new StreamSource(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/ext_simple.xsl")));
        XdmNode source = proc.newDocumentBuilder().build(new StreamSource(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input.xml")));
        XsltTransformer trans = exp.load();
        trans.setInitialContextNode(source);
        final StringWriter output = new StringWriter();
        Serializer out = proc.newSerializer(output);
        out.setOutputProperty(Serializer.Property.METHOD,"xml");
        out.setOutputProperty(Serializer.Property.INDENT, "yes");
        trans.setDestination(out);
        trans.transform();
        System.out.println(output.toString());
    }


    @Test
    public void transformTest() throws Exception {
        final String mdstore_input = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstore").getFile();
        Path tempDirWithPrefix = Files.createTempDirectory("mdstore_output");

        final String mdstore_output = tempDirWithPrefix.toFile().getAbsolutePath()+"/version";

        final String xslt = DHPUtils.compressString(IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml")));

        System.out.println(xslt);
        TransformSparkJobNode.main(new String[]{"-mt","local", "-i", mdstore_input, "-o", mdstore_output,"-d","1", "-w","1","-tr", xslt, "-t", "true", "-ru","", "-rp","", "-rh","", "-ro","", "-rr",""});

        Files.walk(tempDirWithPrefix)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }


    @Test
    public void tryLoadFolderOnCP() throws Exception {
        final String path = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstore").getFile();
        System.out.println("path = " + path);

        Path tempDirWithPrefix = Files.createTempDirectory("mdsotre_output");


        System.out.println(tempDirWithPrefix.toFile().getAbsolutePath());

        Files.deleteIfExists(tempDirWithPrefix);
    }


    @Test
    public void testTransformFunction() throws Exception {
        SAXReader reader = new SAXReader();
        Document document = reader.read(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/tr.xml"));
        Node node = document.selectSingleNode("//CODE/*[local-name()='stylesheet']");
        final String xslt = node.asXML();

        TransformFunction tf = new TransformFunction(accumulator, accumulator, accumulator, xslt, 1);

        MetadataRecord record = new MetadataRecord();
        record.setBody(IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input.xml")));

        final MetadataRecord result = tf.call(record);
        Assert.assertNotNull(result.getBody());

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
