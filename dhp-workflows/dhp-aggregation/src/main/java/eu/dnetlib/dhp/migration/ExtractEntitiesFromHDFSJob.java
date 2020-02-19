package eu.dnetlib.dhp.migration;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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

		final List<String> sourcePaths = Arrays.asList(parser.get("sourcePaths").split(","));
		final String targetPath = parser.get("graphRawPath");

		try (final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
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

		final JavaRDD<String> inputRdd = sc.emptyRDD();
		sourcePaths.forEach(sourcePath -> inputRdd.union(sc.sequenceFile(sourcePath, Text.class, Text.class)
				.map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
				.filter(k -> isEntityType(k._1(), type))
				.map(Tuple2::_2)));

		inputRdd.saveAsTextFile(targetPath + "/" + type);
	}

	private static boolean isEntityType(final String item, final String entity) {
		return StringUtils.substringAfter(item, ":").equalsIgnoreCase(entity);
	}
}
