
package eu.dnetlib.dhp.actionmanager.bipfinder;

import static eu.dnetlib.dhp.actionmanager.createunresolvedentities.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * created the Atomic Action for each tipe of results
 */
public class SparkAtomicActionScoreJob implements Serializable {

	private static final String DOI = "doi";
	private static final Logger log = LoggerFactory.getLogger(SparkAtomicActionScoreJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkAtomicActionScoreJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/bipfinder/input_actionset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}: ", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareResults(spark, inputPath, outputPath);
			});
	}

	private static <I extends Result> void prepareResults(SparkSession spark, String bipScorePath, String outputPath) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<BipDeserialize> bipDeserializeJavaRDD = sc
			.textFile(bipScorePath)
			.map(item -> OBJECT_MAPPER.readValue(item, BipDeserialize.class));

		Dataset<BipScore> bipScores = spark
			.createDataset(bipDeserializeJavaRDD.flatMap(entry -> entry.keySet().stream().map(key -> {
				BipScore bs = new BipScore();
				bs.setId(key);
				bs.setScoreList(entry.get(key));
				return bs;
			}).collect(Collectors.toList()).iterator()).rdd(), Encoders.bean(BipScore.class));

		bipScores

			.map((MapFunction<BipScore, Result>) bs -> {
				Result ret = new Result();

				ret.setId(bs.getId());

				ret.setMeasures(getMeasure(bs));

				return ret;
			}, Encoders.bean(Result.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Result.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static List<Measure> getMeasure(BipScore value) {
		return value
			.getScoreList()
			.stream()
			.map(score -> {
				Measure m = new Measure();
				m.setId(score.getId());
				m
					.setUnit(
						score
							.getUnit()
							.stream()
							.map(unit -> {
								KeyValue kv = new KeyValue();
								kv.setValue(unit.getValue());
								kv.setKey(unit.getKey());
								kv
									.setDataInfo(
										OafMapperUtils
											.dataInfo(
												false,
												UPDATE_DATA_INFO_TYPE,
												true,
												false,
												OafMapperUtils
													.qualifier(
														UPDATE_MEASURE_BIP_CLASS_ID,
														UPDATE_CLASS_NAME,
														ModelConstants.DNET_PROVENANCE_ACTIONS,
														ModelConstants.DNET_PROVENANCE_ACTIONS),
												""));
								return kv;
							})
							.collect(Collectors.toList()));
				return m;
			})
			.collect(Collectors.toList());
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
