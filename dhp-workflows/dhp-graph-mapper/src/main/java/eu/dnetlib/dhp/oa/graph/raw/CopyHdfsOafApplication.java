
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class CopyHdfsOafApplication extends AbstractMigrationApplication {

	private static final Logger log = LoggerFactory.getLogger(CopyHdfsOafApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CopyHdfsOafApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/copy_hdfs_oaf_parameters.json")));
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
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		final List<String> oafTypes = Lists.newArrayList(ModelSupport.oafTypes.keySet());

		runWithSparkSession(conf, isSparkSessionManaged, spark -> processPaths(spark, oafTypes, hdfsPath, paths));
	}

	public static void processPaths(final SparkSession spark,
		final List<String> oafTypes,
		final String outputPath,
		final Set<String> paths) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		log.info("Found {} mdstores", paths.size());
		paths.forEach(log::info);

		final String[] validPaths = paths
			.stream()
			.filter(p -> HdfsSupport.exists(p, sc.hadoopConfiguration()))
			.toArray(String[]::new);
		log.info("Non empty mdstores {}", validPaths.length);

		if (validPaths.length > 0) {
			// load the dataset
			Dataset<Oaf> oaf = spark
				.read()
				.load(validPaths)
				.as(Encoders.kryo(Oaf.class));

			// dispatch each entity type individually in the respective graph subdirectory in append mode
			for (String type : oafTypes) {
				oaf
					.filter((FilterFunction<Oaf>) o -> o.getClass().getSimpleName().toLowerCase().equals(type))
					.map((MapFunction<Oaf, String>) OBJECT_MAPPER::writeValueAsString, Encoders.STRING())
					.write()
					.option("compression", "gzip")
					.mode(SaveMode.Append)
					.text(outputPath + "/" + type);
			}
		}
	}

}
