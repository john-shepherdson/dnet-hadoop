
package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import scala.Tuple2;

public class PrepareResultCommunitySetStep2 {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySetStep2.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultCommunitySetStep2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromsemrel/input_preparecommunitytoresult2_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				mergeInfo(spark, inputPath, outputPath);
			});
	}

	private static void mergeInfo(SparkSession spark, String inputPath, String outputPath) {

		Dataset<ResultCommunityList> resultOrcidAssocCommunityList = readPath(
			spark, inputPath + "/publication", ResultCommunityList.class)
				.union(readPath(spark, inputPath + "/dataset", ResultCommunityList.class))
				.union(readPath(spark, inputPath + "/otherresearchproduct", ResultCommunityList.class))
				.union(readPath(spark, inputPath + "/software", ResultCommunityList.class));

		resultOrcidAssocCommunityList
			.toJavaRDD()
			.mapToPair(r -> new Tuple2<>(r.getResultId(), r))
			.reduceByKey(
				(a, b) -> {
					if (a == null) {
						return b;
					}
					if (b == null) {
						return a;
					}
					Set<String> community_set = new HashSet<>();
					a.getCommunityList().stream().forEach(aa -> community_set.add(aa));
					b
						.getCommunityList()
						.stream()
						.forEach(
							aa -> {
								if (!community_set.contains(aa)) {
									a.getCommunityList().add(aa);
									community_set.add(aa);
								}
							});
					return a;
				})
			.map(Tuple2::_2)
			.map(r -> OBJECT_MAPPER.writeValueAsString(r))
			.saveAsTextFile(outputPath, GzipCodec.class);
	}

}
