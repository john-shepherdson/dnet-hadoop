
package eu.dnetlib.dhp.personprojectthroughdeliverable;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkExec {

	private static final Logger log = LoggerFactory.getLogger(SparkExec.class);
	private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______";
	private static final String PROJECT_ID_PREFIX = ModelSupport.getIdPrefix(Project.class)
			+ IdentifierFactory.ID_PREFIX_SEPARATOR;
	public static final DataInfo DATAINFO = OafMapperUtils
		.dataInfo(
			false,
			"openaire",
			true,
			false,
			OafMapperUtils
				.qualifier(
					ModelConstants.SYSIMPORT_CROSSWALK_REPOSITORY,
					ModelConstants.SYSIMPORT_CROSSWALK_REPOSITORY,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.85");

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
					SparkExec.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/person/input_personpropagation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				exec(
					spark,
					sourcePath,
						workingPath);
			});
	}

	private static void exec(SparkSession spark, String sourcePath,  String workingPath){

		//Project deliverable

		Dataset<Row> deliverables = spark.read().schema(Encoders.bean(Publication.class).schema())
				.json(sourcePath + "/publication")
				.filter(functions.col("instance.instancetype.classname").contains("Project deliverable"))
				.select("id","author","instance")
				;
		//Project reports not clear the classid to be included
		Dataset<Row> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(sourcePath + "/relation")
				.filter("subRelType = 'outcome'")
				.select("source","target");

		deliverables.joinWith(relations, deliverables.col("id").equalTo(relations.col("target")))
				.flatMap((FlatMapFunction<Tuple2<Row, Row>,  Relation>) t2 -> {
					List<Author> authors = t2._1().getAs("author");
					List<Relation> relationList = new ArrayList<>();
					authors.forEach(a -> {
						if(a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid") ||
								p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending")))
							relationList.add(getRelation(a, t2._2().getAs("source")));
					});
					return relationList.iterator();
				} , Encoders.bean(Relation.class))
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

	private static Relation getRelation(Author a, String projectId){
		Optional<StructuredProperty> authorPid = a.getPid().stream().filter(pid -> pid.getQualifier().getClassid().equalsIgnoreCase("orcid")).findFirst();
		String orcid = null;
		if(authorPid.isPresent())
			orcid = authorPid.get().getValue();
		else
			orcid = a.getPid().stream().filter(pid -> pid.getQualifier().getClassid().equalsIgnoreCase("orcid_pending")).findFirst().get().getValue();

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
