
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import scala.Tuple2;

public class PrepareResultOrcidAssociationStep1 {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociationStep1.class);

	public static void main(String[] args) throws Exception {
		String jsonConf = IOUtils
			.toString(
				PrepareResultOrcidAssociationStep1.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_prepareorcidtoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConf);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("workingPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final List<String> allowedsemrel = Arrays
			.stream(parser.get("allowedsemrels").split(";"))
			.map(s -> s.toLowerCase())
			.collect(Collectors.toList());

		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		final List<String> allowedPids = Arrays.asList(parser.get("allowedpids").split(";"));
		log.info("allowedPids: {}", new Gson().toJson(allowedPids));

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				// removeOutputDir(spark, outputPath);
				prepareInfo(
					spark, inputPath, outputPath, resultType, resultClazz, allowedsemrel, allowedPids);
			});
	}

	private static <R extends Result> void prepareInfo(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String resultType,
		Class<R> resultClazz,
		List<String> allowedsemrel,
		List<String> allowedPids) {

		final String inputResultPath = inputPath + "/" + resultType;

		Dataset<Relation> relation = readPath(spark, outputPath + "/relationSubset", Relation.class);

		log.info("Reading Graph table from: {}", inputResultPath);

		final String resultOutputPath = outputPath + "/resultSubset/" + resultType;

		readPath(spark, inputResultPath, resultClazz)
			.filter(
				(FilterFunction<R>) r -> !r.getDataInfo().getDeletedbyinference() && !r.getDataInfo().getInvisible())
			.filter(
				(FilterFunction<R>) r -> Optional
					.ofNullable(r.getAuthor())
					.map(
						al -> al
							.stream()
							.anyMatch(
								a -> hasAllowedPid(a, allowedPids)))
					.orElse(false)

			)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(resultOutputPath);

		Dataset<R> result = readPath(spark, resultOutputPath, resultClazz);

		// result.foreach((ForeachFunction<R>) r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		result
			.joinWith(relation, result.col("id").equalTo(relation.col("source")))
			.map((MapFunction<Tuple2<R, Relation>, ResultOrcidList>) t2 -> {
				ResultOrcidList rol = new ResultOrcidList();
				rol.setResultId(t2._2().getTarget());
				List<AutoritativeAuthor> aal = new ArrayList<>();
				t2._1().getAuthor().stream().forEach(a -> {
					a.getPid().stream().forEach(p -> {
						if (allowedPids.contains(p.getQualifier().getClassid().toLowerCase())) {
							aal
								.add(
									AutoritativeAuthor
										.newInstance(a.getName(), a.getSurname(), a.getFullname(), p.getValue()));
						}
					});
				});
				rol.setAuthorList(aal);
				return rol;
			}, Encoders.bean(ResultOrcidList.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath + "/" + resultType);

	}

	private static boolean hasAllowedPid(Author a, List<String> allowedPids) {
		Optional<List<StructuredProperty>> oPid = Optional.ofNullable(a.getPid());
		if (!oPid.isPresent()) {
			return false;
		}
		return oPid.get().stream().anyMatch(p -> allowedPids.contains(p.getQualifier().getClassid().toLowerCase()));

	}

}
