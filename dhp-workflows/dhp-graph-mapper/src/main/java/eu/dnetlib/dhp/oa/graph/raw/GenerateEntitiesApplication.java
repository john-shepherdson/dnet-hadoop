
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class GenerateEntitiesApplication extends AbstractMigrationApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEntitiesApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	/**
	 * Operation mode
	 */
	enum Mode {

		/**
		 * Groups all the objects by id to merge them
		 */
		claim,

		/**
		 * Default mode
		 */
		graph
	}

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateEntitiesApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/generate_entities_parameters.json")));

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String sourcePaths = parser.get("sourcePaths");
		log.info("sourcePaths: {}", sourcePaths);

		final String targetPath = parser.get("targetPath");
		log.info("targetPath: {}", targetPath);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final boolean shouldHashId = Optional
			.ofNullable(parser.get("shouldHashId"))
			.map(Boolean::valueOf)
			.orElse(true);
		log.info("shouldHashId: {}", shouldHashId);

		final Mode mode = Optional
			.ofNullable(parser.get("mode"))
			.map(Mode::valueOf)
			.orElse(Mode.graph);
		log.info("mode: {}", mode);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookupService);

		final SparkConf conf = new SparkConf();
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			HdfsSupport.remove(targetPath, spark.sparkContext().hadoopConfiguration());
			generateEntities(spark, vocs, sourcePaths, targetPath, shouldHashId, mode);
		});
	}

	private static void generateEntities(
		final SparkSession spark,
		final VocabularyGroup vocs,
		final String sourcePaths,
		final String targetPath,
		final boolean shouldHashId,
		final Mode mode) {

		final List<String> existingSourcePaths = listEntityPaths(spark, sourcePaths);

		log.info("Generate entities from files:");
		existingSourcePaths.forEach(log::info);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<Oaf> inputRdd = sc.emptyRDD();

		for (final String sp : existingSourcePaths) {
			inputRdd = inputRdd
				.union(
					sc
						.sequenceFile(sp, Text.class, Text.class)
						.map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
						.map(k -> convertToListOaf(k._1(), k._2(), shouldHashId, vocs))
						.flatMap(List::iterator)
						.filter(Objects::nonNull));
		}

		switch (mode) {
			case claim:
				save(
					inputRdd
						.mapToPair(oaf -> new Tuple2<>(ModelSupport.idFn().apply(oaf), oaf))
						.reduceByKey(OafMapperUtils::merge)
						.map(Tuple2::_2),
					targetPath);
				break;
			case graph:
				save(inputRdd, targetPath);
				break;
		}
	}

	private static void save(final JavaRDD<Oaf> rdd, final String targetPath) {
		rdd
			.map(
				oaf -> oaf.getClass().getSimpleName().toLowerCase()
					+ "|"
					+ OBJECT_MAPPER.writeValueAsString(oaf))
			.saveAsTextFile(targetPath, GzipCodec.class);
	}

	public static List<Oaf> convertToListOaf(
		final String id,
		final String s,
		final boolean shouldHashId,
		final VocabularyGroup vocs) {
		final String type = StringUtils.substringAfter(id, ":");

		switch (type.toLowerCase()) {
			case "oaf-store-cleaned":
				return new OafToOafMapper(vocs, false, shouldHashId).processMdRecord(s);
			case "oaf-store-claim":
				return new OafToOafMapper(vocs, false, shouldHashId, true).processMdRecord(s);
			case "odf-store-cleaned":
				return new OdfToOafMapper(vocs, false, shouldHashId).processMdRecord(s);
			case "odf-store-claim":
				return new OdfToOafMapper(vocs, false, shouldHashId, true).processMdRecord(s);
			case "oaf-store-intersection":
				return new OafToOafMapper(vocs, true, shouldHashId).processMdRecord(s);
			case "odf-store-intersection":
				return new OdfToOafMapper(vocs, true, shouldHashId).processMdRecord(s);
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
				throw new IllegalArgumentException("type not managed: " + type.toLowerCase());
		}
	}

	private static Oaf convertFromJson(final String s, final Class<? extends Oaf> clazz) {
		try {
			return OBJECT_MAPPER.readValue(s, clazz);
		} catch (final Exception e) {
			log.error("Error parsing object of class: {}:\n{}", clazz, s);
			throw new IllegalArgumentException(e);
		}
	}

}
