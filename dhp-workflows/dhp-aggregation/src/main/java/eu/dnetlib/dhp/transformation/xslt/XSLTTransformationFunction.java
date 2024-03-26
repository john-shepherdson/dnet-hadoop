
package eu.dnetlib.dhp.transformation.xslt;

import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.function.MapFunction;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import net.sf.saxon.s9api.*;

public class XSLTTransformationFunction implements MapFunction<MetadataRecord, MetadataRecord>, Serializable {

	public static final String QNAME_BASE_URI = "http://eu/dnetlib/transform";

	private static final String DATASOURCE_ID_PARAM = "varDataSourceId";

	private static final String DATASOURCE_NAME_PARAM = "varOfficialName";

	private final AggregationCounter aggregationCounter;

	private final String transformationRule;

	private final Cleaner cleanFunction;

	private final long dateOfTransformation;

	private final VocabularyGroup vocabularies;

	public XSLTTransformationFunction(
		final AggregationCounter aggregationCounter,
		final String transformationRule,
		long dateOfTransformation,
		final VocabularyGroup vocabularies) {
		this.aggregationCounter = aggregationCounter;
		this.transformationRule = transformationRule;
		this.vocabularies = vocabularies;
		this.dateOfTransformation = dateOfTransformation;
		cleanFunction = new Cleaner(vocabularies);
	}

	@Override
	public MetadataRecord call(MetadataRecord value) {
		aggregationCounter.getTotalItems().add(1);

		Processor processor = new Processor(false);

		processor.registerExtensionFunction(cleanFunction);
		processor.registerExtensionFunction(new DateCleaner());
		processor.registerExtensionFunction(new PersonCleaner());

		final XsltCompiler comp = processor.newXsltCompiler();
		QName datasourceIDParam = new QName(DATASOURCE_ID_PARAM);
		comp.setParameter(datasourceIDParam, new XdmAtomicValue(value.getProvenance().getDatasourceId()));
		QName datasourceNameParam = new QName(DATASOURCE_NAME_PARAM);
		comp.setParameter(datasourceNameParam, new XdmAtomicValue(value.getProvenance().getDatasourceName()));
		XsltExecutable xslt;
		XdmNode source;
		try {
			xslt = comp
				.compile(new StreamSource(IOUtils.toInputStream(transformationRule, StandardCharsets.UTF_8)));
			source = processor
				.newDocumentBuilder()
				.build(new StreamSource(IOUtils.toInputStream(value.getBody(), StandardCharsets.UTF_8)));
		} catch (Throwable e) {
			throw new RuntimeException("Error on parsing xslt", e);
		}
		try {
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
//			throw new RuntimeException(e);
		}
	}

	public AggregationCounter getAggregationCounter() {
		return aggregationCounter;
	}

	public String getTransformationRule() {
		return transformationRule;
	}

	public Cleaner getCleanFunction() {
		return cleanFunction;
	}

	public long getDateOfTransformation() {
		return dateOfTransformation;
	}

	public VocabularyGroup getVocabularies() {
		return vocabularies;
	}
}
