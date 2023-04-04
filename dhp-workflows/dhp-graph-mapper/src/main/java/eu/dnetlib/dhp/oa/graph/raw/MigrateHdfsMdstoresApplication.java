
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
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
import org.apache.spark.api.java.function.FilterFunction;
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
				.toString(
					MigrateHdfsMdstoresApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_hdfs_mstores_parameters.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String mdstoreManagerUrl = parser.get("mdstoreManagerUrl");
		log.info("mdstoreManagerUrl: {}", mdstoreManagerUrl);

		final String mdFormat = parser.get("mdFormat");
		log.info("mdFormat: {}", mdFormat);

		final String mdLayout = parser.get("mdLayout");
		log.info("mdLayout: {}", mdLayout);

		final String mdInterpretation = parser.get("mdInterpretation");
		log.info("mdInterpretation: {}", mdInterpretation);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

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
		final String type) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		log.info("Found {} not empty mdstores", paths.size());
		paths.forEach(log::info);

		final String[] validPaths = paths
			.stream()
			.filter(p -> HdfsSupport.exists(p, sc.hadoopConfiguration()))
			.toArray(size -> new String[size]);

		log.info("Processing existing paths {}", Arrays.asList(validPaths));

		if (validPaths.length > 0) {
			spark
				.read()
				.parquet(validPaths)
				.map((MapFunction<Row, String>) MigrateHdfsMdstoresApplication::enrichRecord, Encoders.STRING())
				.filter((FilterFunction<String>) Objects::nonNull)
				.toJavaRDD()
				.mapToPair(xml -> new Tuple2<>(new Text(UUID.randomUUID() + ":" + type), new Text(xml)))
				// .coalesce(1)
				.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
		} else {
			spark
				.emptyDataFrame()
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
			final SAXReader reader = new SAXReader();
			reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			final Document doc = reader.read(new StringReader(xml));
			final Element head = (Element) doc.selectSingleNode("//*[local-name() = 'header']");

			head.addElement(new QName("objIdentifier", DRI_NS_PREFIX)).addText(r.getAs("id"));
			head.addElement(new QName("dateOfCollection", DRI_NS_PREFIX)).addText(collDate);
			head.addElement(new QName("dateOfTransformation", DRI_NS_PREFIX)).addText(tranDate);
			return doc.asXML();
		} catch (final Exception e) {
			log.error("Error patching record: " + xml);
			return null;
		}
	}

}
