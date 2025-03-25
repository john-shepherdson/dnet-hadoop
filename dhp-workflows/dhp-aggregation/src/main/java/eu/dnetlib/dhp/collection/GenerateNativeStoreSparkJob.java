
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.CONTENT_INVALIDRECORDS;
import static eu.dnetlib.dhp.common.Constants.CONTENT_TOTALITEMS;
import static eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH;
import static eu.dnetlib.dhp.common.Constants.MDSTORE_SIZE_PATH;
import static eu.dnetlib.dhp.common.Constants.SEQUENCE_FILE_NAME;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.saveDataset;
import static eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.schema.mdstore.ValidationType;
import eu.dnetlib.validator2.validation.XMLApplicationProfile.ValidationResult;
import eu.dnetlib.validator2.validation.guideline.openaire.AbstractOpenAireProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.DataArchiveGuidelinesV2Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.FAIR_Data_GuidelinesProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.FAIR_Literature_GuidelinesV4Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV3Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV4Profile;
import scala.Tuple2;

public class GenerateNativeStoreSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateNativeStoreSparkJob.class);

	private static final ObjectMapper MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateNativeStoreSparkJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/collection/generate_native_input_parameters.json")));
		parser.parseArgument(args);

		final String provenanceArgument = parser.get("provenance");
		log.info("provenance: {}", provenanceArgument);
		final Provenance provenance = MAPPER.readValue(provenanceArgument, Provenance.class);

		final String apiDescriptor = parser.get("apidescriptor");
		log.info("apidescriptor: {}", apiDescriptor);
		final ApiDescriptor api = MAPPER.readValue(apiDescriptor, ApiDescriptor.class);

		final String dateOfCollectionArgs = parser.get("dateOfCollection");
		log.info("dateOfCollection: {}", dateOfCollectionArgs);
		final Long dateOfCollection = Long.valueOf(dateOfCollectionArgs);

		final String mdStoreVersion = parser.get("mdStoreVersion");
		log.info("mdStoreVersion: {}", mdStoreVersion);

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);

		final String readMdStoreVersionParam = parser.get("readMdStoreVersion");
		log.info("readMdStoreVersion: {}", readMdStoreVersionParam);

		final MDStoreVersion readMdStoreVersion = StringUtils.isBlank(readMdStoreVersionParam) ? null
			: MAPPER.readValue(readMdStoreVersionParam, MDStoreVersion.class);

		final String xpath = parser.get("xpath");
		log.info("xpath: {}", xpath);

		final String encoding = parser.get("encoding");
		log.info("encoding: {}", encoding);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SparkConf conf = new SparkConf();

		final Pair<ValidationType, AbstractOpenAireProfile> validator = getValidationType(api.getCompatibilityLevel());

		runWithSparkSession(
			conf, isSparkSessionManaged, spark -> createNativeMDStore(
				spark, provenance, dateOfCollection, xpath, encoding, validator, currentVersion, readMdStoreVersion));
	}

	private static Pair<ValidationType, AbstractOpenAireProfile> getValidationType(final String compatibilityLevel) {
		switch (compatibilityLevel) {
			case "openaire2.0":
				return Pair.of(ValidationType.openaire2_0, new DataArchiveGuidelinesV2Profile());
			case "openaire3.0":
				return Pair.of(ValidationType.openaire3_0, new LiteratureGuidelinesV3Profile());
			case "openaire4.0":
				return Pair.of(ValidationType.openaire4_0, new LiteratureGuidelinesV4Profile());
			case "fair_data":
				return Pair.of(ValidationType.fair_data, new FAIR_Data_GuidelinesProfile());
			case "fair_literature_v4":
				return Pair.of(ValidationType.fair_literature_v4, new FAIR_Literature_GuidelinesV4Profile());
			default:
				throw new IllegalArgumentException("Unknown compatibility level: " + compatibilityLevel);
		}
	}

	private static void createNativeMDStore(final SparkSession spark,
		final Provenance provenance,
		final Long dateOfCollection,
		final String xpath,
		final String encoding,
		final Pair<ValidationType, AbstractOpenAireProfile> validator,
		final MDStoreVersion currentVersion,
		final MDStoreVersion readVersion) throws IOException {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		final LongAccumulator totalItems = sc.sc().longAccumulator(CONTENT_TOTALITEMS);
		final LongAccumulator invalidRecords = sc.sc().longAccumulator(CONTENT_INVALIDRECORDS);

		final String seqFilePath = currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME;

		final JavaRDD<MetadataRecord> nativeStore = sc
			.sequenceFile(seqFilePath, IntWritable.class, Text.class)
			.map(
				item -> parseRecord(
					item._2().toString(), xpath, encoding, provenance, dateOfCollection, totalItems, invalidRecords))
			.filter(Objects::nonNull)
			.distinct();

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> newRecords = spark.createDataset(nativeStore.rdd(), encoder);

		final String targetPath = currentVersion.getHdfsPath() + MDSTORE_DATA_PATH;

		final Dataset<MetadataRecord> toSaveRecords;
		if (readVersion != null) { // INCREMENTAL MODE
			log.info("updating {} incrementally with {}", targetPath, readVersion.getHdfsPath());
			final Dataset<MetadataRecord> oldRecords = spark
				.read()
				.load(readVersion.getHdfsPath() + MDSTORE_DATA_PATH)
				.as(encoder);
			final TypedColumn<MetadataRecord, MetadataRecord> aggregator = new MDStoreAggregator().toColumn();

			toSaveRecords = oldRecords
				.union(newRecords)
				.groupByKey((MapFunction<MetadataRecord, String>) MetadataRecord::getId, Encoders.STRING())
				.agg(aggregator)
				.map((MapFunction<Tuple2<String, MetadataRecord>, MetadataRecord>) Tuple2::_2, encoder);

		} else {
			toSaveRecords = newRecords;
		}

		// ADD THE VALIDATION REPORTS TO ALL THE MDSTORE RECORDS
		final Map<String, LongAccumulator> validationErrors = new LinkedHashMap<>();
		final Map<String, LongAccumulator> validationWarnings = new LinkedHashMap<>();

		validator.getValue().guidelines().forEach(gdl -> {
			validationErrors
				.put(gdl.getName(), sc.sc().longAccumulator(gdl.getName().toLowerCase().replace(' ', '_') + "_errors"));
			validationWarnings
				.put(
					gdl.getName(),
					sc.sc().longAccumulator(gdl.getName().toLowerCase().replace(' ', '_') + "_warnings"));
		});

		final Dataset<MetadataRecord> validated = toSaveRecords
			.map(mdr -> addValidationReport(mdr, validator, validationErrors, validationWarnings), encoder);

		saveDataset(validated, targetPath);

		final Long total = spark.read().load(targetPath).count();
		log.info("collected {} records for datasource '{}'", total, provenance.getDatasourceName());

		writeHdfsFile(
			spark.sparkContext().hadoopConfiguration(), total.toString(),
			currentVersion.getHdfsPath() + MDSTORE_SIZE_PATH);
	}

	public static class MDStoreAggregator extends Aggregator<MetadataRecord, MetadataRecord, MetadataRecord> {

		private static final long serialVersionUID = -3409563083332613984L;

		@Override
		public MetadataRecord zero() {
			return null;
		}

		@Override
		public MetadataRecord reduce(final MetadataRecord b, final MetadataRecord a) {
			return getLatestRecord(b, a);
		}

		@Override
		public MetadataRecord merge(final MetadataRecord b, final MetadataRecord a) {
			return getLatestRecord(b, a);
		}

		private MetadataRecord getLatestRecord(final MetadataRecord b, final MetadataRecord a) {
			if (b == null) {
				return a;
			}

			if (a == null) {
				return b;
			}
			return (a.getDateOfCollection() > b.getDateOfCollection()) ? a : b;
		}

		@Override
		public MetadataRecord finish(final MetadataRecord r) {
			return r;
		}

		@Override
		public Encoder<MetadataRecord> bufferEncoder() {
			return Encoders.bean(MetadataRecord.class);
		}

		@Override
		public Encoder<MetadataRecord> outputEncoder() {
			return Encoders.bean(MetadataRecord.class);
		}

	}

	public static MetadataRecord parseRecord(
		final String input,
		final String xpath,
		final String encoding,
		final Provenance provenance,
		final Long dateOfCollection,
		final LongAccumulator totalItems,
		final LongAccumulator invalidRecords) {

		if (totalItems != null) {
			totalItems.add(1);
		}
		try {
			final SAXReader reader = new SAXReader();
			reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			final Document document = reader.read(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
			final Node node = document.selectSingleNode(xpath);
			final String originalIdentifier = node.getText();
			if (StringUtils.isBlank(originalIdentifier)) {
				if (invalidRecords != null) {
					invalidRecords.add(1);
				}
				return null;
			}

			return new MetadataRecord(originalIdentifier, encoding, provenance, document.asXML(), dateOfCollection);
		} catch (final Throwable e) {
			invalidRecords.add(1);
			return null;
		}
	}

	public static MetadataRecord addValidationReport(final MetadataRecord mdr,
		final Pair<ValidationType, AbstractOpenAireProfile> validator,
		final Map<String, LongAccumulator> errors,
		final Map<String, LongAccumulator> warnings) {

		if (validator == null) {
			return mdr;
		}

		if (mdr.getValidationResults() == null) {
			mdr.setValidationResults(new HashMap<>());
		}

		final ValidationType validationType = validator.getKey();
		final AbstractOpenAireProfile profile = validator.getValue();

		try (final ByteArrayInputStream is = new ByteArrayInputStream(mdr.getBody().getBytes(StandardCharsets.UTF_8))) {
			final org.w3c.dom.Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(is);
			final ValidationResult report = profile.validate(mdr.getId(), doc);
			mdr.getValidationResults().put(validationType, report);
			report.getResults().forEach((name, result) -> {
				if (errors.containsKey(name) && (result.getErrors().size() > 0)) {
					errors.get(name).add(result.getErrors().size()); // TODO discuss if to add the list size or 1
				}
				if (warnings.containsKey(name) && (result.getWarnings().size() > 0)) {
					warnings.get(name).add(result.getWarnings().size()); // TODO discuss if to add the list size or 1
				}
			});
		} catch (final Throwable e) {
			log
				.warn(
					"Error generating validation report, record id: {}, validationType: {}", mdr.getId(),
					validationType, e);
		}

		return mdr;
	}

}
