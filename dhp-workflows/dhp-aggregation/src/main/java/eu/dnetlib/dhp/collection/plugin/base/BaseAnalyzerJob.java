
package eu.dnetlib.dhp.collection.plugin.base;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseAnalyzerJob {

	private static final Logger log = LoggerFactory.getLogger(BaseAnalyzerJob.class);

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
				.toString(BaseAnalyzerJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/collection/plugin/base/action_set_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
				.ofNullable(parser.get("isSparkSessionManaged"))
				.map(Boolean::valueOf)
				.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String dataPath = parser.get("dataPath");
		log.info("dataPath {}: ", dataPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final boolean reimport = Boolean.parseBoolean(parser.get("reimport"));
		log.info("reimport {}: ", reimport);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> processBaseRecords(spark, inputPath, dataPath, outputPath, reimport));
	}

	private static void processBaseRecords(final SparkSession spark,
			final String inputPath,
			final String dataPath,
			final String outputPath,
			final boolean reimport) throws IOException {

		try (final FileSystem fs = FileSystem.get(new Configuration());
				final AggregatorReport report = new AggregatorReport()) {

			if (reimport) {
				fs.delete(new Path(dataPath), true);
				loadRecords(fs, inputPath, dataPath, report);
			}

			fs.delete(new Path(outputPath), true);
			extractInfo(spark, dataPath, outputPath);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

	private static void loadRecords(final FileSystem fs,
			final String inputPath,
			final String outputPath,
			final AggregatorReport report)
			throws Exception {

		final AtomicLong recordsCounter = new AtomicLong(0);

		final LongWritable key = new LongWritable();
		final Text value = new Text();

		try (final SequenceFile.Writer writer = SequenceFile
				.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(LongWritable.class), SequenceFile.Writer
								.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {

			final BaseCollectorIterator iteraror = new BaseCollectorIterator(fs, new Path(inputPath), report);

			while (iteraror.hasNext()) {
				final String record = iteraror.next();

				final long i = recordsCounter.incrementAndGet();
				if ((i % 10000) == 0) {
					log.info("# Loaded records: " + i);
				}

				key.set(i);
				value.set(record);
				try {
					writer.append(key, value);
				} catch (final Throwable e1) {
					throw new RuntimeException(e1);
				}
			}

			log.info("# COMPLETED - Loaded records: " + recordsCounter.get());
		}
	}

	private static void extractInfo(final SparkSession spark,
			final String inputPath,
			final String targetPath) throws Exception {

		final JavaRDD<BaseRecordInfo> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.sequenceFile(inputPath, LongWritable.class, Text.class)
				.map(s -> s._2)
				.map(BaseAnalyzerJob::extractInfo);

		spark.createDataset(rdd.rdd(), Encoders.bean(BaseRecordInfo.class))
				.write()
				.mode(SaveMode.Overwrite)
				.format("parquet")
				.save(targetPath);
	}

	private static BaseRecordInfo extractInfo(final Text s) {
		try {
			final Document record = DocumentHelper.parseText(s.toString());

			final BaseRecordInfo info = new BaseRecordInfo();

			info.setId(record.valueOf("//*[local-name() = 'header']/*[local-name() = 'identifier']").trim());

			for (final Object o : record.selectNodes("//*|//@*")) {
				info.getPaths().add(((Node) o).getPath());

				if (o instanceof Element) {
					final Element n = (Element) o;

					final String nodeName = n.getName();

					if ("collection".equals(nodeName)) {
						final String collName = n.getText().trim();
						if (StringUtils.isNotBlank(collName)) {
							final Map<String, String> attrs = new HashMap<>();
							for (final Object ao : n.attributes()) {
								attrs.put(((Attribute) ao).getName(), ((Attribute) ao).getValue());
							}
							info.getCollections().put(collName, attrs);
						}
					} else if ("type".equals(nodeName)) {
						info.getTypes().add("TYPE: " + n.getText().trim());
					} else if ("typenorm".equals(nodeName)) {
						info.getTypes().add("TYPE_NORM: " + n.getText().trim());
					}
				}
			}
			return info;
		} catch (final DocumentException e) {
			throw new RuntimeException(e);
		}
	}

}
