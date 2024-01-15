
package eu.dnetlib.dhp;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.SparkResultToCommunityFromOrganizationJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 * @author miriam.baglioni
 * @Date 15/01/24
 */
public class MoveResult implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(MoveResult.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToCommunityFromOrganizationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/input_moveresult_parameters.json"));

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
				moveResults(spark, inputPath, outputPath);

			});
	}

	public static <R extends Result> void moveResults(SparkSession spark, String inputPath, String outputPath) {

		ModelSupport.entityTypes
			.keySet()
			.parallelStream()
			.filter(e -> ModelSupport.isResult(e))
			// .parallelStream()
			.forEach(e -> {
				Class<R> resultClazz = ModelSupport.entityTypes.get(e);
				Dataset<R> resultDataset = readPath(spark, inputPath + e.name(), resultClazz);
				if (resultDataset.count() > 0) {

					resultDataset
						.write()
						.mode(SaveMode.Overwrite)
						.option("compression", "gzip")
						.json(outputPath + e.name());
				}

			});

	}

}
