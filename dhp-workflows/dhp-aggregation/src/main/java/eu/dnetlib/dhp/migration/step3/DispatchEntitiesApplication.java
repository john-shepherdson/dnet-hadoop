package eu.dnetlib.dhp.migration.step3;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.migration.step1.MigrateMongoMdstoresApplication;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;

public class DispatchEntitiesApplication {

	private static final Log log = LogFactory.getLog(DispatchEntitiesApplication.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateMongoMdstoresApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/migration/dispatch_entities_parameters.json")));
		parser.parseArgument(args);

		try (final SparkSession spark = newSparkSession(parser); final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

			final String sourcePath = parser.get("sourcePath");
			final String targetPath = parser.get("graphRawPath");

			processEntity(sc, Publication.class, sourcePath, targetPath);
			processEntity(sc, Dataset.class, sourcePath, targetPath);
			processEntity(sc, Software.class, sourcePath, targetPath);
			processEntity(sc, OtherResearchProduct.class, sourcePath, targetPath);
			processEntity(sc, Datasource.class, sourcePath, targetPath);
			processEntity(sc, Organization.class, sourcePath, targetPath);
			processEntity(sc, Project.class, sourcePath, targetPath);
			processEntity(sc, Relation.class, sourcePath, targetPath);
		}
	}

	private static SparkSession newSparkSession(final ArgumentApplicationParser parser) {
		return SparkSession
				.builder()
				.appName(DispatchEntitiesApplication.class.getSimpleName())
				.master(parser.get("master"))
				.getOrCreate();
	}

	private static void processEntity(final JavaSparkContext sc, final Class<?> clazz, final String sourcePath, final String targetPath) {
		final String type = clazz.getSimpleName().toLowerCase();

		log.info(String.format("Processing entities (%s) in file: %s", type, sourcePath));

		sc.textFile(sourcePath)
				.filter(l -> isEntityType(l, type))
				.map(l -> StringUtils.substringAfter(l, "|"))
				.saveAsTextFile(targetPath + "/" + type); // use repartition(XXX) ???
	}

	private static boolean isEntityType(final String line, final String type) {
		return StringUtils.substringBefore(line, "|").equalsIgnoreCase(type);
	}

}
