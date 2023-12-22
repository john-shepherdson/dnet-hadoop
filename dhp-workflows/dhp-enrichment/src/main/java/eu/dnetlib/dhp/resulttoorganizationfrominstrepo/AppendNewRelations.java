
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * @author miriam.baglioni
 * @Date 09/12/23
 */
public class AppendNewRelations implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(AppendNewRelations.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				AppendNewRelations.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/resulttoorganizationfrominstrepo/input_newrelation_parameters.json"));

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
			spark -> appendNewRelation(spark, inputPath, outputPath));
	}

	private static void appendNewRelation(SparkSession spark, String inputPath, String outputPath) {

		readPath(spark, inputPath + "publication/relation", Relation.class)
			.union(readPath(spark, inputPath + "dataset/relation", Relation.class))
			.union(readPath(spark, inputPath + "otherresearchproduct/relation", Relation.class))
			.union(readPath(spark, inputPath + "software/relation", Relation.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
