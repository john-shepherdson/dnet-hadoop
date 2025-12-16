
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.CONTENT_INVALIDRECORDS;
import static eu.dnetlib.dhp.common.Constants.CONTENT_TOTALITEMS;
import static eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH;
import static eu.dnetlib.dhp.common.Constants.MDSTORE_SIZE_PATH;
import static eu.dnetlib.dhp.common.Constants.SEQUENCE_FILE_NAME;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.saveDataset;
import static eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
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
import eu.dnetlib.validator2.result_models.StandardValidationResult;
import eu.dnetlib.validator2.validation.guideline.openaire.AbstractOpenAireProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.DataArchiveGuidelinesV2Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.FAIR_Data_GuidelinesProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.FAIR_Literature_GuidelinesV4Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV3Profile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV4Profile;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import scala.Tuple2;

public class GenerateNativeStoreSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateNativeStoreSparkJob.class);

	private static final ObjectMapper MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static final String VALIDATION_RESULTS_FIELD = "validationResults";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
					.toString(GenerateNativeStoreSparkJob.class
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

		final boolean runValidation = Optional
				.ofNullable(parser.get("runValidation"))
				.map(Boolean::valueOf)
				.orElse(Boolean.FALSE);
		log.info("runValidation: {}", runValidation);

		final SparkConf conf = new SparkConf();

		final Map<ValidationType, AbstractOpenAireProfile> validators = getValidationTypes(api.getCompatibilityLevel(), runValidation);

		runWithSparkSession(conf, isSparkSessionManaged, spark -> createNativeMDStore(spark, provenance, dateOfCollection, xpath, encoding, validators, currentVersion, readMdStoreVersion));
	}

	private static Map<ValidationType, AbstractOpenAireProfile> getValidationTypes(final String compatibilityLevel, boolean runValidation) {

		final Map<ValidationType, AbstractOpenAireProfile> res = new LinkedHashMap<>();

		if (!runValidation) {
			log.info("Skipping validation, returning empty validators map");
			return res;
		}

		switch (compatibilityLevel) {
		case "openaire2.0":
			res.put(ValidationType.openaire2_0, new DataArchiveGuidelinesV2Profile());
			break;
		case "openaire3.0":
			res.put(ValidationType.openaire3_0, new LiteratureGuidelinesV3Profile());
			break;
		case "openaire4.0":
			res.put(ValidationType.openaire4_0, new LiteratureGuidelinesV4Profile());
			res.put(ValidationType.fair_literature_v4, new FAIR_Literature_GuidelinesV4Profile());
			break;
		case "openaire2.0_data":
			res.put(ValidationType.fair_data, new FAIR_Data_GuidelinesProfile());
			break;
		default:
            log.info(String.format("Skipping validation for compatibility: %s", compatibilityLevel));
			return res;
        }

		return res;
	}

	private static void createNativeMDStore(final SparkSession spark,
			final Provenance provenance,
			final Long dateOfCollection,
			final String xpath,
			final String encoding,
			final Map<ValidationType, AbstractOpenAireProfile> validators,
			final MDStoreVersion currentVersion,
			final MDStoreVersion readVersion) throws IOException {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		final LongAccumulator totalItems = sc.sc().longAccumulator(CONTENT_TOTALITEMS);
		final LongAccumulator invalidRecords = sc.sc().longAccumulator(CONTENT_INVALIDRECORDS);

		final String seqFilePath = currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME;

		final JavaRDD<MetadataRecord> nativeStore = sc
				.sequenceFile(seqFilePath, IntWritable.class, Text.class)
				.map(item -> parseRecord(item._2().toString(), xpath, encoding, provenance, dateOfCollection, totalItems, invalidRecords))
				.filter(Objects::nonNull)
				.distinct();

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> newRecords = spark.createDataset(nativeStore.rdd(), encoder);

		final String targetPath = currentVersion.getHdfsPath() + MDSTORE_DATA_PATH;

		final Dataset<MetadataRecord> toSaveRecords;
		if (readVersion != null) { // INCREMENTAL MODE
			log.info("updating {} incrementally with {}", targetPath, readVersion.getHdfsPath());

			// FIX TO INTRODUCE A NEW FIELD

			final DataType dataType = Arrays
					.stream(newRecords
							.schema()
							.fields())
					.filter(f -> VALIDATION_RESULTS_FIELD.equals(f.name()))
					.map(StructField::dataType)
					.findFirst()
					.orElseThrow(() -> new RuntimeException("Missing " + VALIDATION_RESULTS_FIELD + " field in new schema"));

			final Dataset<Row> oldRows = spark.read().load(readVersion.getHdfsPath() + MDSTORE_DATA_PATH);

			final Dataset<Row> oldRowsWithNewField = ArrayUtils
					.contains(oldRows.schema().fieldNames(), VALIDATION_RESULTS_FIELD) ? oldRows
							: oldRows
									.withColumn(VALIDATION_RESULTS_FIELD, functions.lit(null).cast(dataType));

			final Dataset<MetadataRecord> oldRecords = oldRowsWithNewField.as(encoder);
			// END FIX

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
		final Dataset<MetadataRecord> validatedRecords = (validators == null) || validators.isEmpty() ? toSaveRecords
				: toSaveRecords
						.map((MapFunction<MetadataRecord, MetadataRecord>) mdr -> addValidationReports(mdr, validators), encoder);

		saveDataset(validatedRecords, targetPath);

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
			final Document document = reader.read(new StringReader(input));
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

	public static MetadataRecord addValidationReports(final MetadataRecord mdr,
			final Map<ValidationType, AbstractOpenAireProfile> validators) throws ParserConfigurationException, IOException, SAXException {

		if ((validators == null) || validators.isEmpty()) {
			return mdr;
		}

		if (mdr.getValidationResults() == null) {
			mdr.setValidationResults(new LinkedHashMap<>());
		}

        try (final StringReader reader = new StringReader(mdr.getBody())) {
            final org.w3c.dom.Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(reader));

			validators.entrySet().forEach(e -> {
				final ValidationType validationType = e.getKey();
				final AbstractOpenAireProfile profile = e.getValue();
				try {
					final StandardValidationResult report = profile.validate(mdr.getId(), doc);
					final Map<ValidationType, StandardValidationResult> validationResults = new LinkedHashMap<>(mdr.getValidationResults());
					validationResults.put(validationType, report);
					mdr.setValidationResults(validationResults);
				} catch (Exception e1) {
					final String errorMessage = "Error validating record id: " + mdr.getId() + " with profile: " + validationType;
					throw new RuntimeException(errorMessage, e1);
				}
			});
		}

		return mdr;
	}

}
