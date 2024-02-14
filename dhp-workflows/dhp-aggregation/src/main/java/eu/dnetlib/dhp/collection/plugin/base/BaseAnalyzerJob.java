package eu.dnetlib.dhp.collection.plugin.base;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import org.dom4j.Node;
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

		runWithSparkSession(conf, isSparkSessionManaged, spark -> processBaseRecords(spark, inputPath, outputPath));
	}

	private static void processBaseRecords(final SparkSession spark,
			final String inputPath,
			final String outputPath) throws IOException {

		HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());

		try (final FileSystem fs = FileSystem.get(new Configuration()); final AggregatorReport report = new AggregatorReport()) {
			final AtomicInteger recordsCounter = new AtomicInteger(0);
			final Map<String, AtomicInteger> fields = new HashMap<>();
			final Map<String, AtomicInteger> types = new HashMap<>();
			final Map<String, AtomicInteger> collections = new HashMap<>();

			analyze(fs, inputPath, recordsCounter, fields, types, collections, report);
			saveReport(fs, outputPath + "/total", Map.of("#records", recordsCounter));
			saveReport(fs, outputPath + "/fields", fields);
			saveReport(fs, outputPath + "/types", types);
			saveReport(fs, outputPath + "/collections", collections);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

	private static void analyze(final FileSystem fs,
			final String inputPath,
			final AtomicInteger recordsCounter,
			final Map<String, AtomicInteger> fields,
			final Map<String, AtomicInteger> types,
			final Map<String, AtomicInteger> collections,
			final AggregatorReport report) throws JsonProcessingException, IOException {

		final BaseCollectorIterator iteraror = new BaseCollectorIterator(fs, new Path(inputPath), report);

		while (iteraror.hasNext()) {
			final Document record = iteraror.next();

			final int i = recordsCounter.incrementAndGet();
			if ((i % 10000) == 0) {
				log.info("# Read records: " + i);
			}

			final List<String> recTypes = new ArrayList<>();

			for (final Object o : record.selectNodes("//*[local-name()='metadata']//*")) {

				incrementMapCounter(fields, ((Node) o).getPath());

				final String nodeName = ((Node) o).getName();

				if ("collection".equals(nodeName)) {
					final Element n = (Element) o;
					final String collName = n.getText().trim();
					if (StringUtils.isNotBlank(collName)) {
						final Map<String, String> map = new HashMap<>();
						for (final Object ao : n.attributes()) {
							map.put(((Attribute) ao).getName(), ((Attribute) ao).getValue());
						}

						incrementMapCounter(collections, collName + ": " + OBJECT_MAPPER.writeValueAsString(map));
					}
				} else if ("type".equals(nodeName)) {
					recTypes.add("TYPE: " + nodeName);
				} else if ("typenorm".equals(nodeName)) {
					recTypes.add("TYPE_NORM: " + nodeName);
				}
			}

			incrementMapCounter(types, recTypes.stream().sorted().distinct().collect(Collectors.joining(", ")));
		}
	}

	private static void incrementMapCounter(final Map<String, AtomicInteger> map, final String key) {
		if (StringUtils.isNotBlank(key)) {
			if (map.containsKey(key)) {
				map.get(key).incrementAndGet();
			} else {
				map.put(key, new AtomicInteger(1));
			}
		}
	}

	private static void saveReport(final FileSystem fs, final String outputPath, final Map<String, AtomicInteger> fields)
			throws JsonProcessingException, IOException {
		try (final SequenceFile.Writer writer = SequenceFile
				.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(IntWritable.class), SequenceFile.Writer
								.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {

			final Text key = new Text();
			final Text value = new Text();

			for (final Entry<String, AtomicInteger> e : fields.entrySet()) {
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
