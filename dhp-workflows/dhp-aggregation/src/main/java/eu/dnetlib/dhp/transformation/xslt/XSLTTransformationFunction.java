
package eu.dnetlib.dhp.transformation.xslt;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.function.MapFunction;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import net.sf.saxon.s9api.*;

public class XSLTTransformationFunction implements MapFunction<MetadataRecord, MetadataRecord> {

	public final static String QNAME_BASE_URI = "http://eu/dnetlib/transform";

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
			processor.registerExtensionFunction(new DateCleaner());

			final XsltCompiler comp = processor.newXsltCompiler();
			XsltExecutable xslt = comp
				.compile(new StreamSource(IOUtils.toInputStream(transformationRule, StandardCharsets.UTF_8)));
			XdmNode source = processor
				.newDocumentBuilder()
				.build(new StreamSource(IOUtils.toInputStream(value.getBody(), StandardCharsets.UTF_8)));
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
			throw new RuntimeException(e);
		}
	}
}
