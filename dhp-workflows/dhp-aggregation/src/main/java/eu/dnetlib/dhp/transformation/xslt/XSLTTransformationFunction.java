
package eu.dnetlib.dhp.transformation.xslt;

import static eu.dnetlib.dhp.common.Constants.*;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import javax.xml.transform.stream.StreamSource;

import org.apache.avro.test.specialtypes.value;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.spark.api.java.function.MapFunction;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import net.sf.saxon.s9api.*;

public class XSLTTransformationFunction implements MapFunction<MetadataRecord, MetadataRecord>, Serializable {

	public static final String QNAME_BASE_URI = "http://eu/dnetlib/transform";

	private static final String DATASOURCE_ID_PARAM = "varDataSourceId";

	private static final String DATASOURCE_NAME_PARAM = "varOfficialName";

	private final AggregationCounter aggregationCounter;

	private final AggregatorReport report;

	private final String transformationRule;

	private final long dateOfTransformation;

	private final VocabularyGroup vocabularies;

	public XSLTTransformationFunction(
		final AggregationCounter aggregationCounter,
		final AggregatorReport report,
		final String transformationRule,
		long dateOfTransformation,
		final VocabularyGroup vocabularies) {
		this.aggregationCounter = aggregationCounter;
		this.report = report;
		this.transformationRule = transformationRule;
		this.vocabularies = vocabularies;
		this.dateOfTransformation = dateOfTransformation;
	}

	@Override
	public MetadataRecord call(MetadataRecord value) {
		aggregationCounter.getTotalItems().add(1);

		final Processor xsltProcessor = new Processor(false);
		xsltProcessor.registerExtensionFunction(new Cleaner(vocabularies));
		xsltProcessor.registerExtensionFunction(new DateCleaner());
		xsltProcessor.registerExtensionFunction(new PersonCleaner());

		final StringWriter output = new StringWriter();
		final Serializer out = xsltProcessor.newSerializer(output);
		out.setOutputProperty(Serializer.Property.METHOD, "xml");
		out.setOutputProperty(Serializer.Property.INDENT, "yes");

		XsltTransformer transformer;
		try {
			transformer = xsltProcessor
				.newXsltCompiler()
				.compile(new StreamSource(IOUtils.toInputStream(transformationRule, StandardCharsets.UTF_8)))
				.load();
		} catch (SaxonApiException e) {
			report.put(e.getClass().getName(), e.getMessage());
			try {
				report.close();
			} catch (IOException ex) {
				throw new IllegalArgumentException("error compiling the XSLT", e);
			}
			throw new IllegalArgumentException("error compiling the XSLT", e);
		}

		transformer
			.setParameter(new QName(DATASOURCE_ID_PARAM), new XdmAtomicValue(value.getProvenance().getDatasourceId()));
		transformer
			.setParameter(
				new QName(DATASOURCE_NAME_PARAM), new XdmAtomicValue(value.getProvenance().getDatasourceName()));

		try {
			final XdmNode source = xsltProcessor
				.newDocumentBuilder()
				.build(new StreamSource(IOUtils.toInputStream(value.getBody(), StandardCharsets.UTF_8)));

			transformer.setInitialContextNode(source);
			transformer.setDestination(out);
			transformer.transform();
		} catch (SaxonApiException e) {
			report.put(e.getClass().getName(), e.getMessage());
			aggregationCounter.getErrorItems().add(1);
			return null;
		}

		final String xml = output.toString();
		value.setBody(xml);
		value.setDateOfTransformation(dateOfTransformation);
		aggregationCounter.getProcessedItems().add(1);
		return value;
	}

	public AggregationCounter getAggregationCounter() {
		return aggregationCounter;
	}

	public String getTransformationRule() {
		return transformationRule;
	}

	public long getDateOfTransformation() {
		return dateOfTransformation;
	}

	public VocabularyGroup getVocabularies() {
		return vocabularies;
	}
}
