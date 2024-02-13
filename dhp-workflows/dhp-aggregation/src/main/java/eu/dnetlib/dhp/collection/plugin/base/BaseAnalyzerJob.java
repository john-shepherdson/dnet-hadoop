package eu.dnetlib.dhp.collection.plugin.base;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.ror.GenerateRorActionSetJob;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseAnalyzerJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateRorActionSetJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
				.toString(GenerateRorActionSetJob.class
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

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			processBaseRecords(spark, inputPath, outputPath);
		});
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void processBaseRecords(final SparkSession spark,
			final String inputPath,
			final String outputPath) throws IOException {

		try (final FileSystem fs = FileSystem.get(new Configuration()); final AggregatorReport report = new AggregatorReport()) {
			final Map<String, String> collections = new HashMap<>();
			collect(fs, inputPath, outputPath + "/temp", collections, report);
			finalReport(fs, outputPath + "/final", collections);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

	private static void collect(final FileSystem fs,
			final String inputPath,
			final String outputPath,
			final Map<String, String> collections,
			final AggregatorReport report) throws JsonProcessingException, IOException {

		try (final SequenceFile.Writer writer = SequenceFile
				.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(IntWritable.class), SequenceFile.Writer
								.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {

			final AtomicInteger recordsCounter = new AtomicInteger(0);
			final AtomicInteger collectionsCounter = new AtomicInteger(0);

			final BaseCollectorIterator iteraror = new BaseCollectorIterator(fs, new Path(inputPath), report);

			while (iteraror.hasNext()) {
				final Document record = iteraror.next();

				final int i = recordsCounter.incrementAndGet();
				if ((i % 1000) == 0) {
					log.info("# Read records: " + i);
				}

				for (final Object o : record.selectNodes("//*[local-name() = 'collection']")) {

					final Element n = (Element) o;
					final String collName = n.getText().trim();
					if (StringUtils.isNotBlank(collName) && !collections.containsKey(collName)) {
						final Map<String, String> map = new HashMap<>();

						for (final Object ao : n.attributes()) {
							map.put(((Attribute) ao).getName(), ((Attribute) ao).getValue());
						}

						final String attrs = OBJECT_MAPPER.writeValueAsString(map);

						collections.put(collName, attrs);

						final IntWritable key = new IntWritable(collectionsCounter.incrementAndGet());
						final Text value = new Text(collName + ": " + attrs);

						try {
							writer.append(key, value);
						} catch (final Throwable e1) {
							throw new RuntimeException(e1);
						}
					}
				}
			}
		}
	}

	private static void finalReport(final FileSystem fs, final String outputPath, final Map<String, String> collections)
			throws JsonProcessingException, IOException {
		try (final SequenceFile.Writer writer = SequenceFile
				.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(IntWritable.class), SequenceFile.Writer
								.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {

			final Text key = new Text();
			final Text value = new Text();

			for (final Entry<String, String> e : collections.entrySet()) {
				key.set(e.getKey());
				value.set(e.getKey() + ": " + e.getValue());
				try {
					writer.append(key, value);
				} catch (final Throwable e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

}
