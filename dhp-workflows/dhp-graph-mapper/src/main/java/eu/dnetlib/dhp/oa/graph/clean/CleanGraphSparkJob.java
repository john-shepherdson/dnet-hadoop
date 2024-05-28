
package eu.dnetlib.dhp.oa.graph.clean;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.action.model.MasterDuplicate;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class CleanGraphSparkJob {

	private static final Logger log = LoggerFactory.getLogger(CleanGraphSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private ArgumentApplicationParser parser;

	public CleanGraphSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CleanGraphSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		ISLookUpService isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);

		new CleanGraphSparkJob(parser).run(isSparkSessionManaged, isLookup);
	}

	public void run(Boolean isSparkSessionManaged, ISLookUpService isLookUpService)
		throws ISLookUpException, ClassNotFoundException {

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		String contextId = parser.get("contextId");
		log.info("contextId: {}", contextId);

		String verifyParam = parser.get("verifyParam");
		log.info("verifyParam: {}", verifyParam);

		String datasourcePath = parser.get("hostedBy");
		log.info("datasourcePath: {}", datasourcePath);

		String country = parser.get("country");
		log.info("country: {}", country);

		String[] verifyCountryParam = Optional
			.ofNullable(parser.get("verifyCountryParam"))
			.map(s -> s.split(";"))
			.orElse(new String[] {});
		log.info("verifyCountryParam: {}", verifyCountryParam);

		String collectedfrom = parser.get("collectedfrom");
		log.info("collectedfrom: {}", collectedfrom);

		String dsMasterDuplicatePath = parser.get("masterDuplicatePath");
		log.info("masterDuplicatePath: {}", dsMasterDuplicatePath);

		Boolean deepClean = Optional
			.ofNullable(parser.get("deepClean"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);
		log.info("deepClean: {}", deepClean);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookUpService);

		SparkConf conf = new SparkConf();
		conf.setAppName(CleanGraphSparkJob.class.getSimpleName() + "#" + entityClazz.getSimpleName());
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				cleanGraphTable(
					spark, vocs, inputPath, entityClazz, outputPath, contextId, verifyParam, datasourcePath, country,
					verifyCountryParam, collectedfrom, dsMasterDuplicatePath, deepClean);
			});
	}

	private static <T extends Oaf> void cleanGraphTable(
		SparkSession spark,
		VocabularyGroup vocs,
		String inputPath,
		Class<T> clazz,
		String outputPath, String contextId, String verifyParam, String datasourcePath, String country,
		String[] verifyCountryParam, String collectedfrom, String dsMasterDuplicatePath,
		Boolean deepClean) {

		final CleaningRuleMap mapping = CleaningRuleMap.create(vocs);

		final Dataset<T> cleaned_basic = readTableFromPath(spark, inputPath, clazz)
			.map((MapFunction<T, T>) GraphCleaningFunctions::fixVocabularyNames, Encoders.bean(clazz))
			.map((MapFunction<T, T>) value -> OafCleaner.apply(value, mapping), Encoders.bean(clazz))
			.map((MapFunction<T, T>) value -> GraphCleaningFunctions.cleanup(value, vocs), Encoders.bean(clazz))
			.map((MapFunction<T, T>) GraphCleaningFunctions::dedicatedUglyHacks, Encoders.bean(clazz))
			.filter((FilterFunction<T>) GraphCleaningFunctions::filter);

		// read the master-duplicate tuples
		Dataset<MasterDuplicate> md = spark
			.read()
			.textFile(dsMasterDuplicatePath)
			.map(as(MasterDuplicate.class), Encoders.bean(MasterDuplicate.class));

		// prepare the resolved CF|HB references with the corresponding EMPTY master ID
		Dataset<IdCfHbMapping> resolved = spark
			.read()
			.textFile(inputPath)
			.map(as(clazz), Encoders.bean(clazz))
			.flatMap(flattenCfHbFn(), Encoders.bean(IdCfHbMapping.class));

		if (Boolean.FALSE.equals(deepClean)) {

			if (Boolean.TRUE.equals(ModelSupport.isSubClass(clazz, Result.class))) {
				save(fixCFHB(clazz, cleaned_basic, md, resolved), outputPath);
			} else {
				save(cleaned_basic, outputPath);
			}
		} else if (Boolean.TRUE.equals(ModelSupport.isSubClass(clazz, Result.class))) {

			// load the hostedby mapping
			Set<String> hostedBy = Sets
				.newHashSet(
					spark
						.read()
						.textFile(datasourcePath)
						.collectAsList());

			// perform the deep cleaning steps
			final Dataset<T> cleaned_deep = fixCFHB(clazz, cleaned_basic, md, resolved)
				.map(
					(MapFunction<T, T>) value -> GraphCleaningFunctions.cleanContext(value, contextId, verifyParam),
					Encoders.bean(clazz))
				.map(
					(MapFunction<T, T>) value -> GraphCleaningFunctions
						.cleanCountry(value, verifyCountryParam, hostedBy, collectedfrom, country),
					Encoders.bean(clazz));

			save(cleaned_deep, outputPath);
		} else {
			save(cleaned_basic, outputPath);
		}
	}

	private static <T extends Oaf> void save(final Dataset<T> dataset, final String outputPath) {
		dataset
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <T extends Oaf> Dataset<T> fixCFHB(Class<T> clazz, Dataset<T> results, Dataset<MasterDuplicate> md,
		Dataset<IdCfHbMapping> resolved) {

		// set the EMPTY master ID/NAME
		Dataset<IdCfHbMapping> resolvedDs = resolved
			.joinWith(md, resolved.col("cfhb").equalTo(md.col("duplicateId")))
			.map(asIdCfHbMapping(), Encoders.bean(IdCfHbMapping.class))
			.filter((FilterFunction<IdCfHbMapping>) m -> Objects.nonNull(m.getMasterId()));

		return results
			.joinWith(resolvedDs, results.col("id").equalTo(resolvedDs.col("resultId")), "left")
			.groupByKey(
				(MapFunction<Tuple2<T, IdCfHbMapping>, String>) t -> ((Result) t._1()).getId(), Encoders.STRING())
			.mapGroups(getMapGroupsFunction(), Encoders.bean(clazz));
	}

	private static <T extends Oaf> Dataset<T> readTableFromPath(
		SparkSession spark, String inputEntityPath, Class<T> clazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(as(clazz), Encoders.bean(clazz));
	}

	private static <R> MapFunction<String, R> as(Class<R> clazz) {
		return s -> OBJECT_MAPPER.readValue(s, clazz);
	}

	private static <T extends Oaf> FlatMapFunction<T, IdCfHbMapping> flattenCfHbFn() {
		return r -> Stream
			.concat(
				Optional
					.ofNullable(r.getCollectedfrom())
					.map(cf -> cf.stream().map(KeyValue::getKey))
					.orElse(Stream.empty()),
				Stream
					.concat(
						Optional
							.ofNullable(((Result) r).getInstance())
							.map(
								instances -> instances
									.stream()
									.map(i -> Optional.ofNullable(i.getHostedby()).map(KeyValue::getKey).orElse("")))
							.orElse(Stream.empty())
							.filter(StringUtils::isNotBlank),
						Optional
							.ofNullable(((Result) r).getInstance())
							.map(
								instances -> instances
									.stream()
									.map(
										i -> Optional
											.ofNullable(i.getCollectedfrom())
											.map(KeyValue::getKey)
											.orElse("")))
							.orElse(Stream.empty())
							.filter(StringUtils::isNotBlank)))
			.distinct()
			.filter(StringUtils::isNotBlank)
			.map(cfHb -> asIdCfHbMapping(((Result) r).getId(), cfHb))
			.iterator();
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

	private static IdCfHbMapping asIdCfHbMapping(String resultId, String cfHb) {
		IdCfHbMapping m = new IdCfHbMapping(resultId);
		m.setCfhb(cfHb);
		return m;
	}

	private static <T extends Oaf> MapGroupsFunction<String, Tuple2<T, IdCfHbMapping>, T> getMapGroupsFunction() {
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
					filter(res.getCollectedfrom()).forEach(kv -> updateKeyValue(kv, m));
					((Result) res).getInstance().forEach(i -> {
						updateKeyValue(i.getHostedby(), m);
						updateKeyValue(i.getCollectedfrom(), m);
					});
				}
			}

			private Stream<KeyValue> filter(List<KeyValue> kvs) {
				return kvs
					.stream()
					.filter(kv -> StringUtils.isNotBlank(kv.getKey()) && StringUtils.isNotBlank(kv.getValue()));
			}

			private void updateKeyValue(final KeyValue kv, final IdCfHbMapping a) {
				if (Objects.nonNull(kv) && Objects.nonNull(kv.getKey()) && kv.getKey().equals(a.getCfhb())) {
					kv.setKey(a.getMasterId());
					kv.setValue(a.getMasterName());
				}
			}

		};
	}

}
