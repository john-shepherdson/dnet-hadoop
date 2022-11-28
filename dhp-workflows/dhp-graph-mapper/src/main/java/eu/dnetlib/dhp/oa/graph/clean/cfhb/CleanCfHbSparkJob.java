
package eu.dnetlib.dhp.oa.graph.clean.cfhb;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.action.model.MasterDuplicate;
import eu.dnetlib.dhp.oa.graph.clean.country.CleanCountrySparkJob;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;
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

		String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String masterDuplicatePath = parser.get("masterDuplicatePath");
		log.info("masterDuplicatePath: {}", masterDuplicatePath);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends Result> entityClazz = (Class<? extends Result>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				cleanCfHb(
					spark, inputPath, entityClazz, workingPath, masterDuplicatePath, outputPath);
			});
	}

	private static <T extends Result> void cleanCfHb(SparkSession spark, String inputPath, Class<T> entityClazz,
		String workingPath, String masterDuplicatePath, String outputPath) {

		// read the master-duplicate tuples
		Dataset<MasterDuplicate> md = spark
			.read()
			.textFile(masterDuplicatePath)
			.map(as(MasterDuplicate.class), Encoders.bean(MasterDuplicate.class));

		// read the result table
		Dataset<T> res = spark
			.read()
			.textFile(inputPath)
			.map(as(entityClazz), Encoders.bean(entityClazz));

		// prepare the resolved CF|HB references with the corresponding EMPTY master ID
		Dataset<IdCfHbMapping> resolved = res
			.flatMap(
				(FlatMapFunction<T, IdCfHbMapping>) r -> Stream
					.concat(
						r.getCollectedfrom().stream().map(KeyValue::getKey),
						Stream
							.concat(
								r.getInstance().stream().map(Instance::getHostedby).map(KeyValue::getKey),
								r.getInstance().stream().map(Instance::getCollectedfrom).map(KeyValue::getKey)))
					.distinct()
					.map(s -> asIdCfHbMapping(r.getId(), s))
					.iterator(),
				Encoders.bean(IdCfHbMapping.class));

		final String resolvedPath = workingPath + "/cfHbResolved";

		// set the EMPTY master ID and save it aside
		resolved
			.joinWith(md, resolved.col("cfhb").equalTo(md.col("duplicate")))
			.map((MapFunction<Tuple2<IdCfHbMapping, MasterDuplicate>, IdCfHbMapping>) t -> {
				t._1().setMasterId(t._2().getMasterId());
				return t._1();
			}, Encoders.bean(IdCfHbMapping.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(resolvedPath);

		// read again the resolved CF|HB mapping
		Dataset<IdCfHbMapping> resolvedDS = spark
			.read()
			.load(resolvedPath)
			.as(Encoders.bean(IdCfHbMapping.class));

		// Join the results with the resolved CF|HB mapping, apply the mapping and save it
		res
			.joinWith(resolvedDS, res.col("id").equalTo(resolved.col("resultId")), "left")
			.groupByKey((MapFunction<Tuple2<T, IdCfHbMapping>, String>) t -> t._1().getId(), Encoders.STRING())
			.agg(new IdCfHbMappingAggregator(entityClazz).toColumn())
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	public static class IdCfHbMappingAggregator<T extends Result> extends Aggregator<IdCfHbMapping, T, T> {

		private final Class<T> entityClazz;

		public IdCfHbMappingAggregator(Class<T> entityClazz) {
			this.entityClazz = entityClazz;
		}

		@Override
		public T zero() {
			try {
				return entityClazz.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public T reduce(T r, IdCfHbMapping a) {
			if (Objects.isNull(a) && StringUtils.isBlank(a.getMasterId())) {
				return r;
			}
			r.getCollectedfrom().forEach(kv -> updateKeyValue(kv, a));
			r.getInstance().forEach(i -> {
				updateKeyValue(i.getHostedby(), a);
				updateKeyValue(i.getCollectedfrom(), a);
			});
			return r;
		}

		@Override
		public T merge(T b1, T b2) {
			if (Objects.isNull(b1.getId())) {
				return b2;
			}
			return b1;
		}

		@Override
		public T finish(T r) {
			return r;
		}

		private void updateKeyValue(final KeyValue kv, final IdCfHbMapping a) {
			if (kv.getKey().equals(a.getCfhb())) {
				kv.setKey(a.getMasterId());
				kv.setValue(a.getMasterName());
			}
		}

		@Override
		public Encoder<T> bufferEncoder() {
			return Encoders.bean(entityClazz);
		}

		@Override
		public Encoder<T> outputEncoder() {
			return Encoders.bean(entityClazz);
		}
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
