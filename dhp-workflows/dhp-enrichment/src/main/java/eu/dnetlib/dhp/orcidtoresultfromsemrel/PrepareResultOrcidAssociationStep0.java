
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import scala.Tuple2;

public class PrepareResultOrcidAssociationStep0 implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociationStep0.class);

	public static void main(String[] args) throws Exception {
		String jsonConf = IOUtils
			.toString(
				PrepareResultOrcidAssociationStep0.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_prepareorcidtoresult0_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConf);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final List<String> allowedsemrel = Arrays
			.stream(parser.get("allowedsemrels").split(";"))
			.map(s -> s.toLowerCase())
			.collect(Collectors.toList());

		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				selectRelations(
					spark, inputPath, outputPath, allowedsemrel);
			});
	}

	private static void selectRelations(SparkSession spark, String inputPath, String outputPath,
		List<String> allowedsemrel) {

		readPath(spark, inputPath, Relation.class)
			.filter(
				(FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference()
					&& allowedsemrel.contains(r.getRelClass().toLowerCase()))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
