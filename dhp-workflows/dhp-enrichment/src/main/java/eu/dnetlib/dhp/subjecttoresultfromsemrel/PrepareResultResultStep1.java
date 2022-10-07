
package eu.dnetlib.dhp.subjecttoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Subject;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 04/10/22
 *
 * This is for the selection of result with subject in subjetClassList
 */

public class PrepareResultResultStep1 implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultResultStep1.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultResultStep1.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/subjectpropagation/input_preparesubjecttoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);
		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		final String resultType = parser.get("resultType");
		log.info("resultType: {}", resultType);

		final List<String> subjectClassList = Arrays
			.asList(
				parser.get("subjectlist").split(";"))
			.stream()
			.map(s -> s.toLowerCase())
			.collect(Collectors.toList());
		log.info("subjectClassList: {}", subjectClassList);

		final List<String> allowedSemRel = Arrays
			.asList(
				parser.get("allowedSemRel").split(";"))
			.stream()
			.map(s -> s.toLowerCase())
			.collect(Collectors.toList());
		log.info("allowedSemRel: {}", allowedSemRel);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareInfo(spark, inputPath, outputPath, subjectClassList, allowedSemRel, resultClazz, resultType);
			});
	}

	private static <R extends Result> void prepareInfo(SparkSession spark,
		String inputPath,
		String outputPath,
		List<String> subjectClassList,
		List<String> allowedSemRel,
		Class<R> resultClazz,
		String resultType) {

		Dataset<R> result = readPath(spark, inputPath + "/" + resultType, resultClazz)
			.filter(
				(FilterFunction<R>) r -> !r.getDataInfo().getDeletedbyinference() &&
					!r.getDataInfo().getInvisible() &&
					r
						.getSubject()
						.stream()
						.anyMatch(s -> subjectClassList.contains(s.getQualifier().getClassid().toLowerCase())));

		Dataset<Relation> relation = readPath(spark, inputPath + "/relation", Relation.class)
			.filter(
				(FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference() &&
					allowedSemRel.contains(r.getRelClass().toLowerCase()));

		result
			.joinWith(relation, result.col("id").equalTo(relation.col("source")), "right")
			.groupByKey((MapFunction<Tuple2<R, Relation>, String>) t2 -> t2._2().getTarget(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<R, Relation>, ResultSubjectList>) (k,
					it) -> getResultSubjectList(subjectClassList, k, it),
				Encoders.bean(ResultSubjectList.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/" + resultType);
	}

	@NotNull
	private static <R extends Result> ResultSubjectList getResultSubjectList(List<String> subjectClassList, String k,
		Iterator<Tuple2<R, Relation>> it) {
		ResultSubjectList rsl = new ResultSubjectList();
		rsl.setResId(k);
		Tuple2<R, Relation> first = it.next();
		List<SubjectInfo> sbjInfo = new ArrayList<>();
		Set<String> subjectSet = new HashSet<>();
		extracted(subjectClassList, first._1().getSubject(), sbjInfo, subjectSet);
		it.forEachRemaining(t2 -> extracted(subjectClassList, t2._1().getSubject(), sbjInfo, subjectSet));
		rsl.setSubjectList(sbjInfo);
		return rsl;
	}

	private static <R extends Result> void extracted(List<String> subjectClassList, List<Subject> resultSubject,
		List<SubjectInfo> sbjList, Set<String> subjectSet) {

		resultSubject
			.stream()
			.filter(s -> subjectClassList.contains(s.getQualifier().getClassid().toLowerCase()))
			.forEach(s -> {
				if (!subjectSet.contains(s.getValue()))
					sbjList
						.add(
							SubjectInfo
								.newInstance(
									s.getQualifier().getClassid(), s.getQualifier().getClassname(), s.getValue()));
				subjectSet.add(s.getValue());
			});
	}

}
