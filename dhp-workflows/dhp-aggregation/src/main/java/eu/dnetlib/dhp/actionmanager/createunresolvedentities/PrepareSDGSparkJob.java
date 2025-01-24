
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.utils.DHPUtils;

public class PrepareSDGSparkJob implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(PrepareSDGSparkJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareSDGSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/prepare_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final Boolean distributeDOI = Optional
			.ofNullable(parser.get("distributeDoi"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("distribute doi {}", distributeDOI);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				if (distributeDOI)
					doPrepare(
						spark,
						sourcePath,

						outputPath);
				else
					doPrepareoaid(spark, sourcePath, outputPath);

			});
	}

	private static void doPrepare(SparkSession spark, String sourcePath, String outputPath) {
		Dataset<Row> sdgDataset = spark
			.read()
			.format("csv")
			.option("sep", DEFAULT_DELIMITER)
			.option("inferSchema", "true")
			.option("header", "true")
			.option("quotes", "\"")
			.load(sourcePath);

		sdgDataset
			.groupByKey((MapFunction<Row, String>) v -> ((String) v.getAs("doi")).toLowerCase(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Row, Result>) (k,
					it) -> getResult(
						DHPUtils
							.generateUnresolvedIdentifier(
								ModelSupport.entityIdPrefix.get(Result.class.getSimpleName().toLowerCase()) + "|" + k,
								DOI),
						it),
				Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/sdg");
	}

	private static void doPrepareoaid(SparkSession spark, String sourcePath, String outputPath) {
		Dataset<Row> sdgDataset = spark
			.read()
			.format("csv")
			.option("sep", DEFAULT_DELIMITER)
			.option("inferSchema", "true")
			.option("header", "true")
			.option("quotes", "\"")
			.load(sourcePath);
		;

		sdgDataset
			.groupByKey((MapFunction<Row, String>) r -> "50|" + ((String) r.getAs("oaid")), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Row, Result>) PrepareSDGSparkJob::getResult, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/sdg");
	}

	private static @NotNull Result getResult(String id, Iterator<Row> it) {
		Result r = new Result();
		r.setId(id);
		Row first = it.next();
		List<Subject> sbjs = new ArrayList<>();
		sbjs.add(getSubject(first.getAs("sdg"), SDG_CLASS_ID, SDG_CLASS_NAME, UPDATE_SUBJECT_SDG_CLASS_ID));
		it
			.forEachRemaining(
				s -> sbjs
					.add(getSubject(s.getAs("sdg"), SDG_CLASS_ID, SDG_CLASS_NAME, UPDATE_SUBJECT_SDG_CLASS_ID)));
		r.setSubject(sbjs);

		return r;
	}

}
