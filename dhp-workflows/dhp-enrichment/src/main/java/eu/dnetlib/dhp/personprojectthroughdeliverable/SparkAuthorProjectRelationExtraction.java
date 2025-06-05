
package eu.dnetlib.dhp.personprojectthroughdeliverable;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkAuthorProjectRelationExtraction {

	private static final Logger log = LoggerFactory.getLogger(SparkAuthorProjectRelationExtraction.class);
	private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______";


	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
					SparkAuthorProjectRelationExtraction.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/personprojectthroughdeliverable/input_personprojectpropagation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String workingDir = parser.get("workingDir");
		log.info("workingPath: {}", workingDir);

		final String classCodes = parser.get("classCodes");
		log.info("classCodes: {}", classCodes);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				exec(
					spark,
					sourcePath,
						workingDir,
						classCodes);
			});
	}

	private static void exec(SparkSession spark, String sourcePath,  String workingPath, String classCodes){
		String[] classIds = classCodes.split(";");
		Dataset<Row> publications = spark.read().schema(Encoders.bean(Publication.class).schema())
				.json(sourcePath + "/publication");

		Dataset<Row> selectedResults =
				Arrays.stream(classIds).map(classid ->
					publications.filter(functions.array_contains(functions.col("instance.instancetype.classid"), classid))
							.select("id","author","instance")
						).reduce(Dataset::union)
						.orElseGet(spark::emptyDataFrame);

		Dataset<Row> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(sourcePath + "/relation")
				.filter("subRelType = 'outcome'")
				.select("source","target");

		selectedResults.joinWith(relations, selectedResults.col("id").equalTo(relations.col("target")))
				.flatMap((FlatMapFunction<Tuple2<Row, Row>,  Relation>) t2 -> {
					Seq<Row> scalaSeq = t2._1().getAs("author");
					List<Row> authors = JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
					List<Relation> relationList = new ArrayList<>();
					authors.forEach(a -> {
						Seq<Row> scalaSeqPid = a.getAs("pid");
						List<Row> pids = JavaConverters.seqAsJavaListConverter(scalaSeqPid).asJava();
						if(Optional.ofNullable(pids).isPresent()){
							if(pids.stream().anyMatch(p -> {
								Row qualifier = p.getAs("qualifier");
								String classid = qualifier.getAs("classid");
								if(classid.equalsIgnoreCase("orcid") ||
										classid.equalsIgnoreCase("orcid_pending"))
									return true;
								else
									return false;
							}))
								relationList.add(getRelation(a, t2._2().getAs("source")));
						}

					});
					return relationList.iterator();
				} , Encoders.bean(Relation.class))
				.distinct()
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(workingPath + "/relation");

		spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingPath + "/relation")
				.write()
				.mode(SaveMode.Append)
				.option("compression","gzip")
				.json(sourcePath + "/relation");


	}

	private static Relation getRelation(Row a, String projectId){
		Seq<Row> scalaSeqPid = a.getAs("pid");
		List<Row> pids = JavaConverters.seqAsJavaListConverter(scalaSeqPid).asJava();

		Optional<Row> authorPid = pids.stream().filter(pid -> {
			Row qualifier = pid.getAs("qualifier");
			String classid = qualifier.getAs("classid");
			if(classid.equalsIgnoreCase("orcid") )
				return true;
			else
				return false;
		}).findFirst();
		String orcid = null;
		if(authorPid.isPresent())
			orcid = authorPid.get().getAs("value");
		else
			orcid = pids.stream().filter(pid -> {
				Row qualifier = pid.getAs("qualifier");
				String classid = qualifier.getAs("classid");
				if(classid.equalsIgnoreCase("orcid_pending") )
					return true;
				else
					return false;
			}).findFirst().get().getAs("value");

		String source = PERSON_PREFIX + "::" + IdentifierFactory.md5(orcid);

		return OafMapperUtils
				.getRelation(
						source, projectId, ModelConstants.PROJECT_PERSON_RELTYPE, ModelConstants.PROJECT_PERSON_SUBRELTYPE,
						ModelConstants.PROJECT_PERSON_PARTICIPATES,
						null,
						null,
						null);
	}



}
