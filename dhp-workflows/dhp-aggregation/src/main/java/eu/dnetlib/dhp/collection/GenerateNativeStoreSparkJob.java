
package eu.dnetlib.dhp.collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.model.mdstore.Provenance;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.*;
import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class GenerateNativeStoreSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateNativeStoreSparkJob.class);

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateNativeStoreSparkJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/collection_input_parameters.json")));
		parser.parseArgument(args);

		final String provenanceArgument = parser.get("provenance");
		log.info("Provenance is {}", provenanceArgument);
		final Provenance provenance = MAPPER.readValue(provenanceArgument, Provenance.class);

		final String dateOfCollectionArgs = parser.get("dateOfCollection");
		log.info("dateOfCollection is {}", dateOfCollectionArgs);
		final Long dateOfCollection = new Long(dateOfCollectionArgs);

		String mdStoreVersion = parser.get("mdStoreVersion");
		log.info("mdStoreVersion is {}", mdStoreVersion);

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);

		String readMdStoreVersionParam = parser.get("readMdStoreVersion");
		log.info("readMdStoreVersion is {}", readMdStoreVersionParam);

		final MDStoreVersion readMdStoreVersion = StringUtils.isBlank(readMdStoreVersionParam) ? null
			: MAPPER.readValue(readMdStoreVersionParam, MDStoreVersion.class);

		final String xpath = parser.get("xpath");
		log.info("xpath is {}", xpath);

		final String encoding = parser.get("encoding");
		log.info("encoding is {}", encoding);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> createNativeMDStore(
				spark, provenance, dateOfCollection, xpath, encoding, currentVersion, readMdStoreVersion));
	}

	private static void createNativeMDStore(SparkSession spark,
		Provenance provenance,
		Long dateOfCollection,
		String xpath,
		String encoding,
		MDStoreVersion currentVersion,
		MDStoreVersion readVersion) throws IOException {
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		final LongAccumulator totalItems = sc.sc().longAccumulator(CONTENT_TOTALITEMS);
		final LongAccumulator invalidRecords = sc.sc().longAccumulator(CONTENT_INVALIDRECORDS);

		final String seqFilePath = currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		final JavaRDD<MetadataRecord> nativeStore = sc
			.sequenceFile(seqFilePath, IntWritable.class, Text.class)
			.map(
				item -> parseRecord(
					item._2().toString(),
					xpath,
					encoding,
					provenance,
					dateOfCollection,
					totalItems,
					invalidRecords))
			.filter(Objects::nonNull)
			.distinct();

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> mdstore = spark.createDataset(nativeStore.rdd(), encoder);

		final String targetPath = currentVersion.getHdfsPath() + MDSTORE_DATA_PATH;

		if (readVersion != null) { // INCREMENTAL MODE
			log.info("updating {} incrementally with {}", targetPath, readVersion.getHdfsPath());
			Dataset<MetadataRecord> currentMdStoreVersion = spark
				.read()
				.load(readVersion.getHdfsPath() + MDSTORE_DATA_PATH)
				.as(encoder);
			TypedColumn<MetadataRecord, MetadataRecord> aggregator = new MDStoreAggregator().toColumn();

			final Dataset<MetadataRecord> map = currentMdStoreVersion
				.union(mdstore)
				.groupByKey(
					(MapFunction<MetadataRecord, String>) MetadataRecord::getId,
					Encoders.STRING())
				.agg(aggregator)
				.map((MapFunction<Tuple2<String, MetadataRecord>, MetadataRecord>) Tuple2::_2, encoder);

			map.select("id").takeAsList(100).forEach(s -> log.info(s.toString()));

			saveDataset(map, targetPath);

		} else {
			saveDataset(mdstore, targetPath);
		}

		final Long total = spark.read().load(targetPath).count();
		log.info("collected {} records for datasource '{}'", total, provenance.getDatasourceName());

		writeTotalSizeOnHDFS(spark, total, currentVersion.getHdfsPath() + MDSTORE_SIZE_PATH);
	}

	public static class MDStoreAggregator extends Aggregator<MetadataRecord, MetadataRecord, MetadataRecord> {

		@Override
		public MetadataRecord zero() {
			return null;
		}

		@Override
		public MetadataRecord reduce(MetadataRecord b, MetadataRecord a) {
			return getLatestRecord(b, a);
		}

		@Override
		public MetadataRecord merge(MetadataRecord b, MetadataRecord a) {
			return getLatestRecord(b, a);
		}

		private MetadataRecord getLatestRecord(MetadataRecord b, MetadataRecord a) {
			if (b == null)
				return a;

			if (a == null)
				return b;
			return (a.getDateOfCollection() > b.getDateOfCollection()) ? a : b;
		}

		@Override
		public MetadataRecord finish(MetadataRecord r) {
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

		if (totalItems != null)
			totalItems.add(1);
		try {
			SAXReader reader = new SAXReader();
			Document document = reader.read(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
			Node node = document.selectSingleNode(xpath);
			final String originalIdentifier = node.getText();
			if (StringUtils.isBlank(originalIdentifier)) {
				if (invalidRecords != null)
					invalidRecords.add(1);
				return null;
			}
			return new MetadataRecord(originalIdentifier, encoding, provenance, document.asXML(), dateOfCollection);
		} catch (Throwable e) {
			invalidRecords.add(1);
			return null;
		}
	}

}
