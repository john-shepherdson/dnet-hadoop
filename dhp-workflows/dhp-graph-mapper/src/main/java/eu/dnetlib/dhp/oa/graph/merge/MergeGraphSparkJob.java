
package eu.dnetlib.dhp.oa.graph.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import javax.xml.crypto.Data;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.clean.CleanGraphSparkJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

/**
 * Combines the content from two aggregator graph tables of the same type, entities (or relationships) with the same ids
 * are picked preferring those from the BETA aggregator rather then from PROD. The identity of a relationship is defined
 * by eu.dnetlib.dhp.schema.common.ModelSupport#idFn()
 */
public class MergeGraphSparkJob {

	private static final Logger log = LoggerFactory.getLogger(CleanGraphSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String PRIORITY_DEFAULT = "BETA"; // BETA | PROD

	private static final Datasource DATASOURCE = new Datasource();

	static {
		Qualifier compatibility = new Qualifier();
		compatibility.setClassid("UNKNOWN");
		DATASOURCE.setOpenairecompatibility(compatibility);
	}

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanGraphSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/merge_graphs_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		String priority = Optional
			.ofNullable(parser.get("priority"))
			.orElse(PRIORITY_DEFAULT);
		log.info("priority: {}", priority);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String betaInputPath = parser.get("betaInputPath");
		log.info("betaInputPath: {}", betaInputPath);

		String prodInputPath = parser.get("prodInputPath");
		log.info("prodInputPath: {}", prodInputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				mergeGraphTable(spark, priority, betaInputPath, prodInputPath, entityClazz, entityClazz, outputPath);
			});
	}

	private static <P extends Oaf, B extends Oaf> void mergeGraphTable(
		SparkSession spark,
		String priority,
		String betaInputPath,
		String prodInputPath,
		Class<P> p_clazz,
		Class<B> b_clazz,
		String outputPath) {

		Dataset<Tuple2<String, B>> beta = readTableFromPath(spark, betaInputPath, b_clazz);
		Dataset<Tuple2<String, P>> prod = readTableFromPath(spark, prodInputPath, p_clazz);

		prod
			.joinWith(beta, prod.col("_1").equalTo(beta.col("_1")), "full_outer")
			.map((MapFunction<Tuple2<Tuple2<String, P>, Tuple2<String, B>>, P>) value -> {
				Optional<P> p = Optional.ofNullable(value._1()).map(Tuple2::_2);
				Optional<B> b = Optional.ofNullable(value._2()).map(Tuple2::_2);

				if (p.orElse((P) b.orElse((B) DATASOURCE)) instanceof Datasource) {
					return mergeDatasource(p, b);
				}
				switch (priority) {
					default:
					case "BETA":
						return mergeWithPriorityToBETA(p, b);
					case "PROD":
						return mergeWithPriorityToPROD(p, b);
				}
			}, Encoders.bean(p_clazz))
			.filter((FilterFunction<P>) Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	/**
	 * Datasources involved in the merge operation doesn't obey to the infra precedence policy, but relies on a custom
	 * behaviour that, given two datasources from beta and prod returns the one from prod with the highest
	 * compatibility among the two.
	 *
	 * @param p datasource from PROD
	 * @param b datasource from BETA
	 * @param <P> Datasource class type from PROD
	 * @param <B> Datasource class type from BETA
	 * @return the datasource from PROD with the highest compatibility level.
	 */
	protected static <P extends Oaf, B extends Oaf> P mergeDatasource(Optional<P> p, Optional<B> b) {
		if (p.isPresent() & !b.isPresent()) {
			return p.get();
		}
		if (b.isPresent() & !p.isPresent()) {
			return (P) b.get();
		}
		if (!b.isPresent() & !p.isPresent()) {
			return null; // unlikely, at least one should be produced by the join operation
		}

		Datasource dp = (Datasource) p.get();
		Datasource db = (Datasource) b.get();

		List<Qualifier> list = Arrays.asList(dp.getOpenairecompatibility(), db.getOpenairecompatibility());
		dp.setOpenairecompatibility(Collections.min(list, new DatasourceCompatibilityComparator()));
		return (P) dp;
	}

	private static <P extends Oaf, B extends Oaf> P mergeWithPriorityToPROD(Optional<P> p, Optional<B> b) {
		if (b.isPresent() & !p.isPresent()) {
			return (P) b.get();
		}
		if (p.isPresent()) {
			return p.get();
		}
		return null;
	}

	private static <P extends Oaf, B extends Oaf> P mergeWithPriorityToBETA(Optional<P> p, Optional<B> b) {
		if (p.isPresent() & !b.isPresent()) {
			return p.get();
		}
		if (b.isPresent()) {
			return (P) b.get();
		}
		return null;
	}

	private static <T extends Oaf> Dataset<Tuple2<String, T>> readTableFromPath(
		SparkSession spark, String inputEntityPath, Class<T> clazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, Tuple2<String, T>>) value -> {
					final T t = OBJECT_MAPPER.readValue(value, clazz);
					final String id = ModelSupport.idFn().apply(t);
					return new Tuple2<>(id, t);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

}
