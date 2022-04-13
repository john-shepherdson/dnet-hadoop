
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
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
						"/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_prepareorcidtoresult_parameters2.json"));

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
			.groupByKey((MapFunction<ResultOrcidList, String>) rol -> rol.getResultId(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, ResultOrcidList, ResultOrcidList>) (k, it) -> {
				ResultOrcidList resultOrcidList = it.next();
				if (it.hasNext()) {
					Set<String> orcid_set = new HashSet<>();
					resultOrcidList.getAuthorList().stream().forEach(aa -> orcid_set.add(aa.getOrcid()));
					it
						.forEachRemaining(
							val -> val
								.getAuthorList()
								.stream()
								.forEach(
									aa -> {
										if (!orcid_set.contains(aa.getOrcid())) {
											resultOrcidList.getAuthorList().add(aa);
											orcid_set.add(aa.getOrcid());
										}
									}));
				}
				return resultOrcidList;
			}, Encoders.bean(ResultOrcidList.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
