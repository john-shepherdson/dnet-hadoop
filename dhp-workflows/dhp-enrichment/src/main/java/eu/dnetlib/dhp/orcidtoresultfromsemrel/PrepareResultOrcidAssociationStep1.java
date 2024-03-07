
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;

public class PrepareResultOrcidAssociationStep1 {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociationStep1.class);

	public static void main(String[] args) throws Exception {
		String jsonConf = IOUtils
			.toString(
				PrepareResultOrcidAssociationStep1.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/orcidtoresultfromsemrel/input_prepareorcidtoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConf);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		String inputRelationPath = inputPath + "/relation";
		log.info("inputRelationPath: {}", inputRelationPath);

		String inputResultPath = inputPath + "/" + resultType;
		log.info("inputResultPath: {}", inputResultPath);

		String outputResultPath = outputPath + "/" + resultType;
		log.info("outputResultPath: {}", outputResultPath);

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareInfo(
					spark, inputRelationPath, inputResultPath, outputResultPath, resultClazz, allowedsemrel);
			});
	}

	private static <R extends Result> void prepareInfo(
		SparkSession spark,
		String inputRelationPath,
		String inputResultPath,
		String outputResultPath,
		Class<R> resultClazz,
		List<String> allowedsemrel) {

		Dataset<Relation> relation = readPath(spark, inputRelationPath, Relation.class);
		relation.createOrReplaceTempView("relation");

		log.info("Reading Graph table from: {}", inputResultPath);
		Dataset<R> result = readPath(spark, inputResultPath, resultClazz);
		result.createOrReplaceTempView("result");

		String query = "SELECT target resultId, author authorList"
			+ "  FROM (SELECT id, collect_set(named_struct('name', name, 'surname', surname, 'fullname', fullname, 'orcid', orcid)) author "
			+ "        FROM ( "
			+ "               SELECT DISTINCT id, MyT.fullname, MyT.name, MyT.surname, MyP.value orcid "
			+ "               FROM result "
			+ "               LATERAL VIEW EXPLODE (author) a AS MyT "
			+ "               LATERAL VIEW EXPLODE (MyT.pid) p AS MyP "
			+ "               WHERE lower(MyP.qualifier.classid) = '" + ModelConstants.ORCID + "' or "
			+ "                       lower(MyP.qualifier.classid) = '" + ModelConstants.ORCID_PENDING + "') tmp "
			+ "               GROUP BY id) r_t "
			+ " JOIN ("
			+ "        SELECT source, target "
			+ "        FROM relation "
			+ "        WHERE datainfo.deletedbyinference = false "
			+ getConstraintList(" lower(relclass) = '", allowedsemrel)
			+ "              ) rel_rel "
			+ " ON source = id";

		log.info("executedQuery: {}", query);
		spark
			.sql(query)
			.as(Encoders.bean(ResultOrcidList.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputResultPath);
	}

}
