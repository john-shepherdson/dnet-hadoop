package eu.dnetlib.dhp.migration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import scala.Tuple2;

public class ExtractEntitiesFromHDFSJob {

	private static final Log log = LogFactory.getLog(ExtractEntitiesFromHDFSJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateMongoMdstoresApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/migration/extract_entities_from_hdfs_parameters.json")));
		parser.parseArgument(args);

		final SparkSession spark = SparkSession
				.builder()
				.appName(ExtractEntitiesFromHDFSJob.class.getSimpleName())
				.master(parser.get("master"))
				.getOrCreate();

		try (final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

			final List<String> sourcePaths = Arrays.stream(parser.get("sourcePaths").split(",")).filter(p -> exists(sc, p)).collect(Collectors.toList());
			final String targetPath = parser.get("graphRawPath");

			processEntity(sc, Publication.class, sourcePaths, targetPath);
			processEntity(sc, Dataset.class, sourcePaths, targetPath);
			processEntity(sc, Software.class, sourcePaths, targetPath);
			processEntity(sc, OtherResearchProduct.class, sourcePaths, targetPath);
			processEntity(sc, Datasource.class, sourcePaths, targetPath);
			processEntity(sc, Organization.class, sourcePaths, targetPath);
			processEntity(sc, Project.class, sourcePaths, targetPath);
			processEntity(sc, Relation.class, sourcePaths, targetPath);
		}
	}

	private static void processEntity(final JavaSparkContext sc, final Class<?> clazz, final List<String> sourcePaths, final String targetPath) {
		final String type = clazz.getSimpleName().toLowerCase();

		log.info(String.format("Processing entities (%s) in files:", type));
		sourcePaths.forEach(log::info);

		JavaRDD<String> inputRdd = sc.emptyRDD();

		for (final String sp : sourcePaths) {
			inputRdd = inputRdd.union(sc.sequenceFile(sp, Text.class, Text.class)
					.map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
					.filter(k -> isEntityType(k._1(), type))
					.map(Tuple2::_2));
		}

		inputRdd.saveAsTextFile(targetPath + "/" + type);

	}

	private static boolean isEntityType(final String item, final String type) {
		return StringUtils.substringAfter(item, ":").equalsIgnoreCase(type);
	}

	private static boolean exists(final JavaSparkContext context, final String pathToFile) {
		try {
			final FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(context.hadoopConfiguration());
			final Path path = new Path(pathToFile);
			return hdfs.exists(path);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}
}
