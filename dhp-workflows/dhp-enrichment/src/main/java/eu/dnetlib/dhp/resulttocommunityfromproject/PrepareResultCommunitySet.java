
package eu.dnetlib.dhp.resulttocommunityfromproject;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.api.model.CommunityEntityMap;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultOrganizations;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PrepareResultCommunitySet {

	private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySet.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultCommunitySet.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromproject/input_preparecommunitytoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String baseURL = parser.get("baseURL");
		log.info("baseURL: {}", baseURL);

		final CommunityEntityMap projectsMap = Utils.getCommunityProjects(baseURL);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareInfo(spark, inputPath, outputPath, projectsMap);
			});
	}

	private static void prepareInfo(
		SparkSession spark,
		String inputPath,
		String outputPath,
		CommunityEntityMap projectMap) {

		final StructType structureSchema = new StructType()
			.add(
				"dataInfo", new StructType()
					.add("deletedbyinference", DataTypes.BooleanType)
					.add("invisible", DataTypes.BooleanType))
			.add("source", DataTypes.StringType)
			.add("target", DataTypes.StringType)
			.add("relClass", DataTypes.StringType);

		spark
			.read()
			.schema(structureSchema)
			.json(inputPath)
			.filter(
				"dataInfo.deletedbyinference != true " +
					"and relClass == '" + ModelConstants.IS_PRODUCED_BY + "'")
			.select(
				new Column("source").as("resultId"),
				new Column("target").as("projectId"))
			.groupByKey((MapFunction<Row, String>) r -> (String) r.getAs("resultId"), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Row, ResultProjectList>) (k, v) -> {
				ResultProjectList rpl = new ResultProjectList();
				rpl.setResultId(k);
				ArrayList<String> cl = new ArrayList<>();
				cl.addAll(projectMap.get(v.next().getAs("projectId")));
				v.forEachRemaining(r -> {
					projectMap
						.get(r.getAs("projectId"))
						.forEach(c -> {
							if (!cl.contains(c))
								cl.add(c);
						});

				});
				if (cl.size() == 0)
					return null;
				rpl.setCommunityList(cl);
				return rpl;
			}, Encoders.bean(ResultProjectList.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
