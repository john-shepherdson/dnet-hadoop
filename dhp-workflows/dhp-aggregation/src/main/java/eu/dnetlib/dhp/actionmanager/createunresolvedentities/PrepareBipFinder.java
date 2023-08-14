
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.actionmanager.Constants.UPDATE_CLASS_NAME;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.bipmodel.BipScore;
import eu.dnetlib.dhp.actionmanager.bipmodel.score.deserializers.BipResultModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;

public class PrepareBipFinder implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareBipFinder.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareBipFinder.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/prepare_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String sourcePath = parser.get("sourcePath");
		log.info("sourcePath {}: ", sourcePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				prepareResults(spark, sourcePath, outputPath);
			});
	}

	private static void prepareResults(SparkSession spark, String inputPath, String outputPath) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<BipResultModel> bipDeserializeJavaRDD = sc
			.textFile(inputPath)
			.map(item -> OBJECT_MAPPER.readValue(item, BipResultModel.class));

		spark
			.createDataset(bipDeserializeJavaRDD.flatMap(entry -> entry.keySet().stream().map(key -> {
				BipScore bs = new BipScore();
				bs.setId(key);
				bs.setScoreList(entry.get(key));

				return bs;
			}).collect(Collectors.toList()).iterator()).rdd(), Encoders.bean(BipScore.class))
			.map((MapFunction<BipScore, Result>) v -> {
				Result r = new Result();
				final String cleanedPid = CleaningFunctions.normalizePidValue(DOI, v.getId());

				r.setId(DHPUtils.generateUnresolvedIdentifier(v.getId(), DOI));
				Instance inst = new Instance();
				inst.setMeasures(getMeasure(v));

				inst
					.setPid(
						Arrays
							.asList(
								OafMapperUtils
									.structuredProperty(
										cleanedPid,
										OafMapperUtils
											.qualifier(
												DOI, DOI_CLASSNAME,
												ModelConstants.DNET_PID_TYPES,
												ModelConstants.DNET_PID_TYPES),
										null)));
				r.setInstance(Arrays.asList(inst));
				r
					.setDataInfo(
						OafMapperUtils
							.dataInfo(
								false, null, true,
								false,
								OafMapperUtils
									.qualifier(
										ModelConstants.PROVENANCE_ENRICH,
										null,
										ModelConstants.DNET_PROVENANCE_ACTIONS,
										ModelConstants.DNET_PROVENANCE_ACTIONS),
								null));
				return r;
			}, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/bip");
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
}
