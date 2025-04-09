
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class PrepareFOSSparkJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareFOSSparkJob.class);

	private static final String RESULT_ID_PREFIX = ModelSupport.entityIdPrefix
		.get(Result.class.getSimpleName().toLowerCase()) + IdentifierFactory.ID_PREFIX_SEPARATOR;

	private static final String DOI_PREFIX = "doi_________::";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareFOSSparkJob.class
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
			spark -> processFOS(spark, sourcePath, outputPath));
	}

	private static void processFOS(SparkSession spark, String sourcePath, String outputPath) {
		readJsonFromPath(spark, sourcePath, FOSDataModel.class)
			.groupByKey((MapFunction<FOSDataModel, String>) PrepareFOSSparkJob::createIdentifier, Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, FOSDataModel, Result>) PrepareFOSSparkJob::getResult,
				Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/fos");
	}

	private static String createIdentifier(FOSDataModel v) throws JsonProcessingException {
		if (StringUtils.isNotBlank(v.getDoi())) {
			final String doi = PidCleaner.normalizePidValue(PidType.doi.toString(), v.getDoi());
			return RESULT_ID_PREFIX + DOI_PREFIX + IdentifierFactory.md5(doi);
		}
		if (StringUtils.isNotBlank(v.getOaid())) {
			final String oaid = v.getOaid();
			return StringUtils.startsWith(oaid, RESULT_ID_PREFIX) ? oaid : RESULT_ID_PREFIX + oaid;
		}
		throw new RuntimeException("No identifier found for FOSDataModel: " + OBJECT_MAPPER.writeValueAsString(v));
	}

	@NotNull
	private static Result getResult(String id, Iterator<FOSDataModel> it) {
		Result r = new Result();
		FOSDataModel first = it.next();
		r.setId(id);

		HashSet<String> level1 = new HashSet<>();
		HashSet<String> level2 = new HashSet<>();
		HashSet<String> level3 = new HashSet<>();
		HashSet<String> level4 = new HashSet<>();
		addLevels(level1, level2, level3, level4, first);
		it.forEachRemaining(v -> addLevels(level1, level2, level3, level4, v));
		List<Subject> sbjs = new ArrayList<>();
		level1
			.forEach(l -> add(sbjs, getSubject(l, FOS_CLASS_ID, FOS_CLASS_NAME, UPDATE_SUBJECT_FOS_CLASS_ID)));
		level2
			.forEach(l -> add(sbjs, getSubject(l, FOS_CLASS_ID, FOS_CLASS_NAME, UPDATE_SUBJECT_FOS_CLASS_ID)));
		level3
			.forEach(
				l -> add(sbjs, getSubject(l, FOS_CLASS_ID, FOS_CLASS_NAME, UPDATE_SUBJECT_FOS_CLASS_ID, true)));
		level4
			.forEach(
				l -> add(sbjs, getSubject(l, FOS_CLASS_ID, FOS_CLASS_NAME, UPDATE_SUBJECT_FOS_CLASS_ID, true)));
		r.setSubject(sbjs);

		return r;
	}

	private static void add(List<Subject> sbsjs, Subject sbj) {
		if (sbj != null)
			sbsjs.add(sbj);
	}

	private static void addLevels(HashSet<String> level1, HashSet<String> level2, HashSet<String> level3,
		HashSet<String> level4,
		FOSDataModel first) {
		level1.add(first.getLevel1());
		level2.add(first.getLevel2());
		if (Optional.ofNullable(first.getLevel3()).isPresent() &&
			!first.getLevel3().equalsIgnoreCase(NA) && !first.getLevel3().equalsIgnoreCase(NULL)
			&& first.getLevel3() != null)
			level3.add(first.getLevel3() + "@@" + first.getScoreL3());
		else
			level3.add(NULL);
		if (Optional.ofNullable(first.getLevel4()).isPresent() &&
			!first.getLevel4().equalsIgnoreCase(NA) &&
			!first.getLevel4().equalsIgnoreCase(NULL) &&
			first.getLevel4() != null)
			level4.add(first.getLevel4() + "@@" + first.getScoreL4());
		else
			level4.add(NULL);
	}

}
