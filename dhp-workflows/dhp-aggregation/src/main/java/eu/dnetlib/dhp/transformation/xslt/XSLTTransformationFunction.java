
package eu.dnetlib.dhp.transformation.xslt;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import net.sf.saxon.s9api.*;
import org.apache.spark.api.java.function.MapFunction;

import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;

public class XSLTTransformationFunction implements MapFunction<MetadataRecord, MetadataRecord> {

    private final AggregationCounter aggregationCounter;

    private final String transformationRule;

    private final Cleaner cleanFunction;

    private final long dateOfTransformation;

    public XSLTTransformationFunction(
            final AggregationCounter aggregationCounter,
            final String transformationRule,
            long dateOfTransformation,
            final VocabularyGroup vocabularies)
            throws Exception {
        this.aggregationCounter = aggregationCounter;
        this.transformationRule = transformationRule;
        this.dateOfTransformation = dateOfTransformation;
        cleanFunction = new Cleaner(vocabularies);
    }

    @Override
    public MetadataRecord call(MetadataRecord value) {
        aggregationCounter.getTotalItems().add(1);
        try {
            Processor processor = new Processor(false);
            processor.registerExtensionFunction(cleanFunction);
            final XsltCompiler comp = processor.newXsltCompiler();
            XsltExecutable xslt = comp
                    .compile(new StreamSource(new ByteArrayInputStream(transformationRule.getBytes())));
            XdmNode source = processor
                    .newDocumentBuilder()
                    .build(new StreamSource(new ByteArrayInputStream(value.getBody().getBytes())));
            XsltTransformer trans = xslt.load();
            trans.setInitialContextNode(source);
            final StringWriter output = new StringWriter();
            Serializer out = processor.newSerializer(output);
            out.setOutputProperty(Serializer.Property.METHOD, "xml");
            out.setOutputProperty(Serializer.Property.INDENT, "yes");
            trans.setDestination(out);
            trans.transform();
            final String xml = output.toString();
            value.setBody(xml);
            value.setDateOfTransformation(dateOfTransformation);
            aggregationCounter.getProcessedItems().add(1);
            return value;
        } catch (Throwable e) {
            aggregationCounter.getErrorItems().add(1);
            return null;
        }
    }
}
