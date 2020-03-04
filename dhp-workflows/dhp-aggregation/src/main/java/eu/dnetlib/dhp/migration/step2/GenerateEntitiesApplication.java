package eu.dnetlib.dhp.migration.step2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.migration.step1.MigrateMongoMdstoresApplication;
import eu.dnetlib.dhp.migration.utils.DbClient;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import scala.Tuple2;

public class GenerateEntitiesApplication {

	private static final Log log = LogFactory.getLog(GenerateEntitiesApplication.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateMongoMdstoresApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/migration/generate_entities_parameters.json")));

		parser.parseArgument(args);

		final String sourcePaths = parser.get("sourcePaths");
		final String targetPath = parser.get("targetPath");

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final Map<String, String> code2name = loadClassNames(dbUrl, dbUser, dbPassword);

		try (final SparkSession spark = newSparkSession(parser); final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
			final List<String> existingSourcePaths = Arrays.stream(sourcePaths.split(",")).filter(p -> exists(sc, p)).collect(Collectors.toList());
			generateEntities(sc, code2name, existingSourcePaths, targetPath);
		}
	}

	private static SparkSession newSparkSession(final ArgumentApplicationParser parser) {
		return SparkSession
				.builder()
				.appName(GenerateEntitiesApplication.class.getSimpleName())
				.master(parser.get("master"))
				.getOrCreate();
	}

	private static void generateEntities(final JavaSparkContext sc,
			final Map<String, String> code2name,
			final List<String> sourcePaths,
			final String targetPath) {

		log.info("Generate entities from files:");
		sourcePaths.forEach(log::info);

		JavaRDD<String> inputRdd = sc.emptyRDD();

		for (final String sp : sourcePaths) {
			inputRdd = inputRdd.union(sc.sequenceFile(sp, Text.class, Text.class)
					.map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
					.map(k -> convertToListOaf(k._1(), k._2(), code2name))
					.flatMap(list -> list.iterator())
					.map(oaf -> oaf.getClass().getSimpleName().toLowerCase() + "|" + convertToJson(oaf)));
		}

		inputRdd.saveAsTextFile(targetPath);

	}

	private static List<Oaf> convertToListOaf(final String id, final String s, final Map<String, String> code2name) {
		final String type = StringUtils.substringAfter(id, ":");

		switch (type.toLowerCase()) {
		case "native_oaf":
			return new OafToOafMapper(code2name).processMdRecord(s);
		case "native_odf":
			return new OdfToOafMapper(code2name).processMdRecord(s);
		case "datasource":
			return Arrays.asList(convertFromJson(s, Datasource.class));
		case "organization":
			return Arrays.asList(convertFromJson(s, Organization.class));
		case "project":
			return Arrays.asList(convertFromJson(s, Project.class));
		case "relation":
			return Arrays.asList(convertFromJson(s, Relation.class));
		case "publication":
			return Arrays.asList(convertFromJson(s, Publication.class));
		case "dataset":
			return Arrays.asList(convertFromJson(s, Dataset.class));
		case "software":
			return Arrays.asList(convertFromJson(s, Software.class));
		case "otherresearchproducts":
		default:
			return Arrays.asList(convertFromJson(s, OtherResearchProduct.class));
		}

	}

	private static Map<String, String> loadClassNames(final String dbUrl, final String dbUser, final String dbPassword) throws IOException {

		log.info("Loading vocabulary terms from db...");

		final Map<String, String> map = new HashMap<>();

		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {
			dbClient.processResults("select code, name from class", rs -> {
				try {
					map.put(rs.getString("code"), rs.getString("name"));
				} catch (final SQLException e) {
					e.printStackTrace();
				}
			});
		}

		log.info("Found " + map.size() + " terms.");

		return map;

	}

	private static String convertToJson(final Oaf oaf) {
		try {
			return new ObjectMapper().writeValueAsString(oaf);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Oaf convertFromJson(final String s, final Class<? extends Oaf> clazz) {
		try {
			return new ObjectMapper().readValue(s, clazz);
		} catch (final Exception e) {
			log.error("Error parsing object of class: " + clazz);
			log.error(s);
			throw new RuntimeException(e);
		}
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
