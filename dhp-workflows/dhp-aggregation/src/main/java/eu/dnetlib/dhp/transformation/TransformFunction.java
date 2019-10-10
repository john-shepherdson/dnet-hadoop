package eu.dnetlib.dhp.transformation;

import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.functions.Cleaner;
import net.sf.saxon.s9api.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;

import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;

public class TransformFunction implements MapFunction<MetadataRecord, MetadataRecord> {


    private final LongAccumulator totalItems;
    private final LongAccumulator errorItems;
    private final LongAccumulator transformedItems;
    private final String trasformationRule;


    private final long dateOfTransformation;


    public TransformFunction(LongAccumulator totalItems, LongAccumulator errorItems, LongAccumulator transformedItems, final String trasformationRule, long dateOfTransformation) throws Exception {
        this.totalItems= totalItems;
        this.errorItems = errorItems;
        this.transformedItems = transformedItems;
        this.trasformationRule = trasformationRule;
        this.dateOfTransformation = dateOfTransformation;
    }

    @Override
    public MetadataRecord call(MetadataRecord value) {
        totalItems.add(1);
        try {
            final Cleaner cleanFunction = new Cleaner();
            Processor processor = new Processor(false);
            processor.registerExtensionFunction(cleanFunction);
            final XsltCompiler comp = processor.newXsltCompiler();
            XsltExecutable xslt = comp.compile(new StreamSource(new ByteArrayInputStream(trasformationRule.getBytes())));
            XdmNode source = processor.newDocumentBuilder().build(new StreamSource(new ByteArrayInputStream(value.getBody().getBytes())));
            XsltTransformer trans = xslt.load();
            trans.setInitialContextNode(source);
            final StringWriter output = new StringWriter();
            Serializer out = processor.newSerializer(output);
            out.setOutputProperty(Serializer.Property.METHOD,"xml");
            out.setOutputProperty(Serializer.Property.INDENT, "yes");
            trans.setDestination(out);
            trans.transform();
            final String xml = output.toString();
            value.setBody(xml);
            value.setDateOfCollection(dateOfTransformation);
            transformedItems.add(1);
            return value;
        }catch (Throwable e) {
            errorItems.add(1);
            return null;
        }
    }



}