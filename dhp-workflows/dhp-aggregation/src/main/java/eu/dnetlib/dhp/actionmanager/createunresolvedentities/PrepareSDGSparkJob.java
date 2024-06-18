
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.SDGDataModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
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

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				doPrepare(
					spark,
					sourcePath,

					outputPath);
			});
	}

	private static void doPrepare(SparkSession spark, String sourcePath, String outputPath) {
		Dataset<SDGDataModel> sdgDataset = readPath(spark, sourcePath, SDGDataModel.class);

		sdgDataset
			.groupByKey((MapFunction<SDGDataModel, String>) r -> r.getDoi().toLowerCase(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, SDGDataModel, Result>) (k, it) -> {
				Result r = new Result();
				r.setId(DHPUtils.generateUnresolvedIdentifier(k, DOI));
				SDGDataModel first = it.next();
				List<Subject> sbjs = new ArrayList<>();
				sbjs.add(getSubject(first.getSbj(), SDG_CLASS_ID, SDG_CLASS_NAME, UPDATE_SUBJECT_SDG_CLASS_ID));
				it
					.forEachRemaining(
						s -> sbjs
							.add(getSubject(s.getSbj(), SDG_CLASS_ID, SDG_CLASS_NAME, UPDATE_SUBJECT_SDG_CLASS_ID)));
				r.setSubject(sbjs);

				return r;
			}, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/sdg");
	}

}
