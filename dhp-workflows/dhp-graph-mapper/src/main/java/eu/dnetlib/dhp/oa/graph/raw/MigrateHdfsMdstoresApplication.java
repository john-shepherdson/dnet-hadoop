
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Namespace;
import org.dom4j.QName;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.mdstore.MDStoreWithInfo;
import scala.Tuple2;

public class MigrateHdfsMdstoresApplication extends AbstractMigrationApplication {

	private static final Logger log = LoggerFactory.getLogger(MigrateHdfsMdstoresApplication.class);
	private static final Namespace DRI_NS_PREFIX = new Namespace("dri",
		"http://www.driver-repository.eu/namespace/dri");

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(MigrateHdfsMdstoresApplication.class
					.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_hdfs_mstores_parameters.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String mdstoreManagerUrl = parser.get("mdstoreManagerUrl");
		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");

		final Set<String> paths = mdstorePaths(mdstoreManagerUrl, mdFormat, mdLayout, mdInterpretation);

		final SparkConf conf = new SparkConf();
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			HdfsSupport.remove(hdfsPath, spark.sparkContext().hadoopConfiguration());
			processPaths(spark, hdfsPath, paths, String.format("%s-%s-%s", mdFormat, mdLayout, mdInterpretation));
		});
	}

	public static void processPaths(final SparkSession spark,
		final String outputPath,
		final Set<String> paths,
		final String type) throws Exception {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		log.info("Found " + paths.size() + " not empty mdstores");
		paths.forEach(log::info);

		final String[] validPaths = paths
			.stream()
			.filter(p -> HdfsSupport.exists(p, sc.hadoopConfiguration()))
			.toArray(size -> new String[size]);

		if (validPaths.length > 0) {
			spark
				.read()
				.parquet(validPaths)
				.map((MapFunction<Row, String>) r -> enrichRecord(r), Encoders.STRING())
				.toJavaRDD()
				.mapToPair(xml -> new Tuple2<>(new Text(UUID.randomUUID() + ":" + type), new Text(xml)))
				// .coalesce(1)
				.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
		} else {
			spark.emptyDataFrame()
				.toJavaRDD()
				.mapToPair(xml -> new Tuple2<>(new Text(), new Text()))
				.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
		}
	}

	private static String enrichRecord(final Row r) {
		final String xml = r.getAs("body");

		final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
		final String collDate = dateFormat.format(new Date((Long) r.getAs("dateOfCollection")));
		final String tranDate = dateFormat.format(new Date((Long) r.getAs("dateOfTransformation")));

		try {
			final Document doc = new SAXReader().read(new StringReader(xml));
			final Element head = (Element) doc.selectSingleNode("//*[local-name() = 'header']");
			head.addElement(new QName("objIdentifier", DRI_NS_PREFIX)).addText(r.getAs("id"));
			head.addElement(new QName("dateOfCollection", DRI_NS_PREFIX)).addText(collDate);
			head.addElement(new QName("dateOfTransformation", DRI_NS_PREFIX)).addText(tranDate);
			return doc.asXML();
		} catch (final Exception e) {
			log.error("Error patching record: " + xml);
			throw new RuntimeException("Error patching record: " + xml, e);
		}
	}

	private static Set<String> mdstorePaths(final String mdstoreManagerUrl,
		final String format,
		final String layout,
		final String interpretation)
		throws Exception {
		final String url = mdstoreManagerUrl + "/mdstores/";
		final ObjectMapper objectMapper = new ObjectMapper();

		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				final String json = IOUtils.toString(response.getEntity().getContent());
				final MDStoreWithInfo[] mdstores = objectMapper.readValue(json, MDStoreWithInfo[].class);
				return Arrays
					.stream(mdstores)
					.filter(md -> md.getFormat().equalsIgnoreCase(format))
					.filter(md -> md.getLayout().equalsIgnoreCase(layout))
					.filter(md -> md.getInterpretation().equalsIgnoreCase(interpretation))
					.filter(md -> StringUtils.isNotBlank(md.getHdfsPath()))
					.filter(md -> StringUtils.isNotBlank(md.getCurrentVersion()))
					.filter(md -> md.getSize() > 0)
					.map(md -> md.getHdfsPath() + "/" + md.getCurrentVersion() + "/store")
					.collect(Collectors.toSet());
			}
		}
	}
}
