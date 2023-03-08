
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class VerifyRecordsApplication extends AbstractMigrationApplication {

	private static final Logger log = LoggerFactory.getLogger(VerifyRecordsApplication.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					VerifyRecordsApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/verify_records_parameters.json")));

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String sourcePaths = parser.get("sourcePaths");
		log.info("sourcePaths: {}", sourcePaths);

		final String invalidPath = parser.get("invalidPath");
		log.info("invalidPath: {}", invalidPath);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookupService);

		final SparkConf conf = new SparkConf();
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			HdfsSupport.remove(invalidPath, spark.sparkContext().hadoopConfiguration());
			validateRecords(spark, sourcePaths, invalidPath, vocs);
		});
	}

	private static void validateRecords(SparkSession spark, String sourcePaths, String invalidPath,
		VocabularyGroup vocs) {

		final List<String> existingSourcePaths = listEntityPaths(spark, sourcePaths);

		log.info("Verify records in files:");
		existingSourcePaths.forEach(log::info);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		for (final String sp : existingSourcePaths) {
			RDD<String> invalidRecords = sc
				.sequenceFile(sp, Text.class, Text.class)
				.map(k -> tryApplyMapping(k._1().toString(), k._2().toString(), true, vocs))
				.filter(Objects::nonNull)
				.rdd();
			spark
				.createDataset(invalidRecords, Encoders.STRING())
				.write()
				.mode(SaveMode.Append)
				.option("compression", "gzip")
				.text(invalidPath);
		}
	}

	private static String tryApplyMapping(
		final String id,
		final String xmlRecord,
		final boolean shouldHashId,
		final VocabularyGroup vocs) {

		final List<Oaf> oaf = GenerateEntitiesApplication.convertToListOaf(id, xmlRecord, shouldHashId, vocs);
		if (Optional.ofNullable(oaf).map(List::isEmpty).orElse(false)) {
			return xmlRecord;
		}
		return null;
	}
}
