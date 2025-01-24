
package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelSupport.isSubClass;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

/** Applies a given action payload file to graph table of compatible type. */
public class PromoteActionPayloadForGraphTableJob {
	private static final Logger logger = LoggerFactory.getLogger(PromoteActionPayloadForGraphTableJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PromoteActionPayloadForGraphTableJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/promote/promote_action_payload_for_graph_table_input_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputGraphTablePath = parser.get("inputGraphTablePath");
		logger.info("inputGraphTablePath: {}", inputGraphTablePath);

		String graphTableClassName = parser.get("graphTableClassName");
		logger.info("graphTableClassName: {}", graphTableClassName);

		String inputActionPayloadPath = parser.get("inputActionPayloadPath");
		logger.info("inputActionPayloadPath: {}", inputActionPayloadPath);

		String actionPayloadClassName = parser.get("actionPayloadClassName");
		logger.info("actionPayloadClassName: {}", actionPayloadClassName);

		String outputGraphTablePath = parser.get("outputGraphTablePath");
		logger.info("outputGraphTablePath: {}", outputGraphTablePath);

		MergeAndGet.Strategy mergeAndGetStrategy = MergeAndGet.Strategy
			.valueOf(parser.get("mergeAndGetStrategy").toUpperCase());
		logger.info("mergeAndGetStrategy: {}", mergeAndGetStrategy);

		Boolean shouldGroupById = Optional
			.ofNullable(parser.get("shouldGroupById"))
			.map(Boolean::valueOf)
			.orElse(true);
		logger.info("shouldGroupById: {}", shouldGroupById);

		PromoteAction.Strategy promoteActionStrategy = Optional
			.ofNullable(parser.get("promoteActionStrategy"))
			.map(PromoteAction.Strategy::valueOf)
			.orElse(PromoteAction.Strategy.UPSERT);
		logger.info("promoteActionStrategy: {}", promoteActionStrategy);

		@SuppressWarnings("unchecked")
		Class<? extends Oaf> rowClazz = (Class<? extends Oaf>) Class.forName(graphTableClassName);
		@SuppressWarnings("unchecked")
		Class<? extends Oaf> actionPayloadClazz = (Class<? extends Oaf>) Class.forName(actionPayloadClassName);

		throwIfGraphTableClassIsNotSubClassOfActionPayloadClass(rowClazz, actionPayloadClazz);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputGraphTablePath);
				promoteActionPayloadForGraphTable(
					spark,
					inputGraphTablePath,
					inputActionPayloadPath,
					outputGraphTablePath,
					mergeAndGetStrategy,
					promoteActionStrategy,
					rowClazz,
					actionPayloadClazz,
					shouldGroupById);
			});
	}

	private static void throwIfGraphTableClassIsNotSubClassOfActionPayloadClass(
		Class<? extends Oaf> rowClazz, Class<? extends Oaf> actionPayloadClazz) {
		if (!isSubClass(rowClazz, actionPayloadClazz)) {
			String msg = String
				.format(
					"graph table class is not a subclass of action payload class: graph=%s, action=%s",
					rowClazz.getCanonicalName(), actionPayloadClazz.getCanonicalName());
			throw new RuntimeException(msg);
		}
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static <G extends Oaf, A extends Oaf> void promoteActionPayloadForGraphTable(
		SparkSession spark,
		String inputGraphTablePath,
		String inputActionPayloadPath,
		String outputGraphTablePath,
		MergeAndGet.Strategy mergeAndGetStrategy,
		PromoteAction.Strategy promoteActionStrategy,
		Class<G> rowClazz,
		Class<A> actionPayloadClazz, Boolean shouldGroupById) {
		Dataset<G> rowDS = readGraphTable(spark, inputGraphTablePath, rowClazz);
		Dataset<A> actionPayloadDS = readActionPayload(spark, inputActionPayloadPath, actionPayloadClazz);

		Dataset<G> result = promoteActionPayloadForGraphTable(
			rowDS, actionPayloadDS, mergeAndGetStrategy, promoteActionStrategy, rowClazz, actionPayloadClazz,
			shouldGroupById)
				.map((MapFunction<G, G>) value -> value, Encoders.bean(rowClazz));

		saveGraphTable(result, outputGraphTablePath);
	}

	private static <G extends Oaf> Dataset<G> readGraphTable(
		SparkSession spark, String path, Class<G> rowClazz) {
		logger.info("Reading graph table from path: {}", path);

		if (HdfsSupport.exists(path, spark.sparkContext().hadoopConfiguration())) {
			return spark
				.read()
				.textFile(path)
				.map(
					(MapFunction<String, G>) value -> OBJECT_MAPPER.readValue(value, rowClazz),
					Encoders.bean(rowClazz));
		} else {
			logger.info("Found empty graph table from path: {}", path);
			return spark.emptyDataset(Encoders.bean(rowClazz));
		}
	}

	private static <A extends Oaf> Dataset<A> readActionPayload(
		SparkSession spark, String path, Class<A> actionPayloadClazz) {
		logger.info("Reading action payload from path: {}", path);

		return spark
			.read()
			.parquet(path)
			.map((MapFunction<Row, String>) PromoteActionPayloadForGraphTableJob::extractPayload, Encoders.STRING())
			.map(
				(MapFunction<String, A>) value -> decodePayload(actionPayloadClazz, value),
				Encoders.bean(actionPayloadClazz));
	}

	private static String extractPayload(Row value) {
		try {
			return value.getAs("payload");
		} catch (IllegalArgumentException | ClassCastException e) {
			logger.error("cannot extract payload from action: {}", value);
			throw e;
		}
	}

	private static <A extends Oaf> A decodePayload(Class<A> actionPayloadClazz, String payload) throws IOException {
		try {
			return OBJECT_MAPPER.readValue(payload, actionPayloadClazz);
		} catch (UnrecognizedPropertyException e) {
			logger.error("error decoding payload: {}", payload);
			throw e;
		}
	}

	private static <G extends Oaf, A extends Oaf> Dataset<G> promoteActionPayloadForGraphTable(
		Dataset<G> rowDS,
		Dataset<A> actionPayloadDS,
		MergeAndGet.Strategy mergeAndGetStrategy,
		PromoteAction.Strategy promoteActionStrategy,
		Class<G> rowClazz,
		Class<A> actionPayloadClazz,
		Boolean shouldGroupById) {
		logger
			.info(
				"Promoting action payload for graph table: payload={}, table={}",
				actionPayloadClazz.getSimpleName(),
				rowClazz.getSimpleName());

		SerializableSupplier<Function<G, String>> rowIdFn = ModelSupport::idFn;
		SerializableSupplier<Function<A, String>> actionPayloadIdFn = ModelSupport::idFn;
		SerializableSupplier<BiFunction<G, A, G>> mergeRowWithActionPayloadAndGetFn = MergeAndGet
			.functionFor(mergeAndGetStrategy);
		SerializableSupplier<BiFunction<G, G, G>> mergeRowsAndGetFn = MergeAndGet.functionFor(mergeAndGetStrategy);
		SerializableSupplier<G> zeroFn = zeroFn(rowClazz);
		SerializableSupplier<Function<G, Boolean>> isNotZeroFn = PromoteActionPayloadForGraphTableJob::isNotZeroFnUsingIdOrSourceAndTarget;

		Dataset<G> joinedAndMerged = PromoteActionPayloadFunctions
			.joinGraphTableWithActionPayloadAndMerge(
				rowDS,
				actionPayloadDS,
				rowIdFn,
				actionPayloadIdFn,
				mergeRowWithActionPayloadAndGetFn,
				promoteActionStrategy,
				rowClazz,
				actionPayloadClazz);

		if (Boolean.TRUE.equals(shouldGroupById)) {
			return PromoteActionPayloadFunctions
				.groupGraphTableByIdAndMerge(
					joinedAndMerged, rowIdFn, mergeRowsAndGetFn, zeroFn, isNotZeroFn, rowClazz);
		} else {
			return joinedAndMerged;
		}
	}

	private static <T extends Oaf> SerializableSupplier<T> zeroFn(Class<T> clazz) {
		switch (clazz.getCanonicalName()) {
			case "eu.dnetlib.dhp.schema.oaf.Dataset":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Dataset());
			case "eu.dnetlib.dhp.schema.oaf.Datasource":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Datasource());
			case "eu.dnetlib.dhp.schema.oaf.Organization":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Organization());
			case "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.OtherResearchProduct());
			case "eu.dnetlib.dhp.schema.oaf.Project":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Project());
			case "eu.dnetlib.dhp.schema.oaf.Publication":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Publication());
			case "eu.dnetlib.dhp.schema.oaf.Relation":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Relation());
			case "eu.dnetlib.dhp.schema.oaf.Software":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Software());
			case "eu.dnetlib.dhp.schema.oaf.Person":
				return () -> clazz.cast(new eu.dnetlib.dhp.schema.oaf.Person());
			default:
				throw new RuntimeException("unknown class: " + clazz.getCanonicalName());
		}
	}

	private static <T extends Oaf> Function<T, Boolean> isNotZeroFnUsingIdOrSourceAndTarget() {
		return t -> {
			if (isSubClass(t, Relation.class)) {
				final Relation rel = (Relation) t;
				return StringUtils.isNotBlank(rel.getSource()) && StringUtils.isNotBlank(rel.getTarget());
			}
			return StringUtils.isNotBlank(((OafEntity) t).getId());
		};
	}

	private static <G extends Oaf> void saveGraphTable(Dataset<G> result, String path) {
		logger.info("Saving graph table to path: {}", path);
		result.toJSON().write().option("compression", "gzip").text(path);
	}
}
