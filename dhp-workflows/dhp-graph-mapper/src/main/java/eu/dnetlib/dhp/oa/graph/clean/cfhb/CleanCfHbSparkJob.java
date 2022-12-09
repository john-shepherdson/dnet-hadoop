
package eu.dnetlib.dhp.oa.graph.clean.cfhb;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.action.model.MasterDuplicate;
import eu.dnetlib.dhp.oa.graph.clean.country.CleanCountrySparkJob;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class CleanCfHbSparkJob {

	private static final Logger log = LoggerFactory.getLogger(CleanCfHbSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanCountrySparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_clean_cfhb_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String resolvedPath = parser.get("resolvedPath");
		log.info("resolvedPath: {}", resolvedPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String dsMasterDuplicatePath = parser.get("masterDuplicatePath");
		log.info("masterDuplicatePath: {}", dsMasterDuplicatePath);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends Result> entityClazz = (Class<? extends Result>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				HdfsSupport.remove(resolvedPath, spark.sparkContext().hadoopConfiguration());
				cleanCfHb(
					spark, inputPath, entityClazz, resolvedPath, dsMasterDuplicatePath, outputPath);
			});
	}

	private static <T extends Result> void cleanCfHb(SparkSession spark, String inputPath, Class<T> entityClazz,
		String resolvedPath, String masterDuplicatePath, String outputPath) {

		// read the master-duplicate tuples
		Dataset<MasterDuplicate> md = spark
			.read()
			.textFile(masterDuplicatePath)
			.map(as(MasterDuplicate.class), Encoders.bean(MasterDuplicate.class));

		// prepare the resolved CF|HB references with the corresponding EMPTY master ID
		Dataset<IdCfHbMapping> resolved = spark
			.read()
			.textFile(inputPath)
			.map(as(entityClazz), Encoders.bean(entityClazz))
			.flatMap(flattenCfHbFn(), Encoders.bean(IdCfHbMapping.class));

		// set the EMPTY master ID/NAME and save it
		resolved
			.joinWith(md, resolved.col("cfhb").equalTo(md.col("duplicateId")))
			.map(asIdCfHbMapping(), Encoders.bean(IdCfHbMapping.class))
			.filter((FilterFunction<IdCfHbMapping>) m -> Objects.nonNull(m.getMasterId()))
			.write()
			.mode(SaveMode.Overwrite)
			.json(resolvedPath);

		// read again the resolved CF|HB mapping
		Dataset<IdCfHbMapping> resolvedDS = spark
			.read()
			.textFile(resolvedPath)
			.map(as(IdCfHbMapping.class), Encoders.bean(IdCfHbMapping.class));

		// read the result table
		Dataset<T> res = spark
			.read()
			.textFile(inputPath)
			.map(as(entityClazz), Encoders.bean(entityClazz));

		// Join the results with the resolved CF|HB mapping, apply the mapping and save it
		res
			.joinWith(resolvedDS, res.col("id").equalTo(resolvedDS.col("resultId")), "left")
			.groupByKey((MapFunction<Tuple2<T, IdCfHbMapping>, String>) t -> t._1().getId(), Encoders.STRING())
			.mapGroups(getMapGroupsFunction(), Encoders.bean(entityClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static MapFunction<Tuple2<IdCfHbMapping, MasterDuplicate>, IdCfHbMapping> asIdCfHbMapping() {
		return t -> {
			final IdCfHbMapping mapping = t._1();
			Optional
				.ofNullable(t._2())
				.ifPresent(t2 -> {
					mapping.setMasterId(t2.getMasterId());
					mapping.setMasterName(t2.getMasterName());

				});
			return mapping;
		};
	}

	private static <T extends Result> FlatMapFunction<T, IdCfHbMapping> flattenCfHbFn() {
		return r -> Stream
			.concat(
				r.getCollectedfrom().stream().map(KeyValue::getKey),
				Stream
					.concat(
						r.getInstance().stream().map(Instance::getHostedby).map(KeyValue::getKey),
						r.getInstance().stream().map(Instance::getCollectedfrom).map(KeyValue::getKey)))
			.distinct()
			.filter(StringUtils::isNotBlank)
			.map(cfHb -> asIdCfHbMapping(r.getId(), cfHb))
			.iterator();
	}

	private static <T extends Result> MapGroupsFunction<String, Tuple2<T, IdCfHbMapping>, T> getMapGroupsFunction() {
		return new MapGroupsFunction<String, Tuple2<T, IdCfHbMapping>, T>() {
			@Override
			public T call(String key, Iterator<Tuple2<T, IdCfHbMapping>> values) {
				final Tuple2<T, IdCfHbMapping> first = values.next();
				final T res = first._1();

				updateResult(res, first._2());
				values.forEachRemaining(t -> updateResult(res, t._2()));
				return res;
			}

			private void updateResult(T res, IdCfHbMapping m) {
				if (Objects.nonNull(m)) {
					res.getCollectedfrom().forEach(kv -> updateKeyValue(kv, m));
					res.getInstance().forEach(i -> {
						updateKeyValue(i.getHostedby(), m);
						updateKeyValue(i.getCollectedfrom(), m);
					});
				}
			}

			private void updateKeyValue(final KeyValue kv, final IdCfHbMapping a) {
				if (kv.getKey().equals(a.getCfhb())) {
					kv.setKey(a.getMasterId());
					kv.setValue(a.getMasterName());
				}
			}

		};
	}

	private static IdCfHbMapping asIdCfHbMapping(String resultId, String cfHb) {
		IdCfHbMapping m = new IdCfHbMapping(resultId);
		m.setCfhb(cfHb);
		return m;
	}

	private static <R> MapFunction<String, R> as(Class<R> clazz) {
		return s -> OBJECT_MAPPER.readValue(s, clazz);
	}
}
