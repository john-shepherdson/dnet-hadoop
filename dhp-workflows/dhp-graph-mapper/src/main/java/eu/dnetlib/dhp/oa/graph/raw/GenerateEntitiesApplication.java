
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.raw.common.DbClient;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class GenerateEntitiesApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEntitiesApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateMongoMdstoresApplication.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/generate_entities_parameters.json")));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String sourcePaths = parser.get("sourcePaths");
		final String targetPath = parser.get("targetPath");

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final Map<String, String> code2name = loadClassNames(dbUrl, dbUser, dbPassword);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, targetPath);
				generateEntities(spark, code2name, sourcePaths, targetPath);
			});
	}

	private static void generateEntities(
		final SparkSession spark,
		final Map<String, String> code2name,
		final String sourcePaths,
		final String targetPath) {

		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		final List<String> existingSourcePaths = Arrays
			.stream(sourcePaths.split(","))
			.filter(p -> exists(sc, p))
			.collect(Collectors.toList());

		log.info("Generate entities from files:");
		existingSourcePaths.forEach(log::info);

		JavaRDD<Oaf> inputRdd = sc.emptyRDD();

		for (final String sp : existingSourcePaths) {
			inputRdd = inputRdd
				.union(
					sc
						.sequenceFile(sp, Text.class, Text.class)
						.map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
						.map(k -> convertToListOaf(k._1(), k._2(), code2name))
						.flatMap(list -> list.iterator()));
		}

		inputRdd
			.mapToPair(oaf -> new Tuple2<>(ModelSupport.idFn().apply(oaf), oaf))
			.reduceByKey((o1, o2) -> merge(o1, o2))
			.map(Tuple2::_2)
			.map(
				oaf -> oaf.getClass().getSimpleName().toLowerCase()
					+ "|"
					+ OBJECT_MAPPER.writeValueAsString(oaf))
			.saveAsTextFile(targetPath, GzipCodec.class);
	}

	private static Oaf merge(Oaf o1, Oaf o2) {
		if (ModelSupport.isSubClass(o1, OafEntity.class)) {
			((OafEntity) o1).mergeFrom((OafEntity) o2);
		} else if (ModelSupport.isSubClass(o1, Relation.class)) {
			((Relation) o1).mergeFrom((Relation) o2);
		} else {
			throw new RuntimeException("invalid Oaf type:" + o1.getClass().getCanonicalName());
		}
		return o1;
	}

	private static List<Oaf> convertToListOaf(
		final String id, final String s, final Map<String, String> code2name) {
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
			case "otherresearchproduct":
				return Arrays.asList(convertFromJson(s, OtherResearchProduct.class));
			default:
				throw new RuntimeException("type not managed: " + type.toLowerCase());
		}
	}

	private static Map<String, String> loadClassNames(
		final String dbUrl, final String dbUser, final String dbPassword) throws IOException {

		log.info("Loading vocabulary terms from db...");

		final Map<String, String> map = new HashMap<>();

		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {
			dbClient
				.processResults(
					"select code, name from class",
					rs -> {
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

	private static Oaf convertFromJson(final String s, final Class<? extends Oaf> clazz) {
		try {
			return OBJECT_MAPPER.readValue(s, clazz);
		} catch (final Exception e) {
			log.error("Error parsing object of class: " + clazz);
			log.error(s);
			throw new RuntimeException(e);
		}
	}

	private static boolean exists(final JavaSparkContext context, final String pathToFile) {
		try {
			final FileSystem hdfs = FileSystem.get(context.hadoopConfiguration());
			final Path path = new Path(pathToFile);
			return hdfs.exists(path);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
