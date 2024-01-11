
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import scala.Tuple2;

public class PrepareResultOrcidAssociationStep2 {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociationStep2.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultOrcidAssociationStep2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/orcidtoresultfromsemrel/input_prepareorcidtoresult_parameters2.json"));

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

		Dataset<ResultOrcidList> resultOrcidAssoc = readPath(spark, inputPath + "/publication", ResultOrcidList.class)
			.union(readPath(spark, inputPath + "/dataset", ResultOrcidList.class))
			.union(readPath(spark, inputPath + "/otherresearchproduct", ResultOrcidList.class))
			.union(readPath(spark, inputPath + "/software", ResultOrcidList.class));

		resultOrcidAssoc
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
					Set<String> orcid_set = new HashSet<>();
					a.getAuthorList().stream().forEach(aa -> orcid_set.add(aa.getOrcid()));
					b
						.getAuthorList()
						.stream()
						.forEach(
							aa -> {
								if (!orcid_set.contains(aa.getOrcid())) {
									a.getAuthorList().add(aa);
									orcid_set.add(aa.getOrcid());
								}
							});
					return a;
				})
			.map(Tuple2::_2)
			.map(r -> OBJECT_MAPPER.writeValueAsString(r))
			.saveAsTextFile(outputPath, GzipCodec.class);
	}

}
