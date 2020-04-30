
package eu.dnetlib.dhp;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.oaf.*;

public class PropagationConstant {
	public static final String INSTITUTIONAL_REPO_TYPE = "pubsrepository::institutional";

	public static final String PROPAGATION_DATA_INFO_TYPE = "propagation";

	public static final String TRUE = "true";

	public static final String DNET_COUNTRY_SCHEMA = "dnet:countries";
	public static final String DNET_SCHEMA_NAME = "dnet:provenanceActions";
	public static final String DNET_SCHEMA_ID = "dnet:provenanceActions";

	public static final String PROPAGATION_COUNTRY_INSTREPO_CLASS_ID = "country:instrepos";
	public static final String PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME = "Propagation of country to result collected from datasources of type institutional repositories";

	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID = "result:organization:instrepo";
	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME = "Propagation of affiliation to result collected from datasources of type institutional repository";

	public static final String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID = "result:project:semrel";
	public static final String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME = "Propagation of result to project through semantic relation";

	public static final String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID = "result:community:semrel";
	public static final String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME = " Propagation of result belonging to community through semantic relation";

	public static final String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID = "result:community:organization";
	public static final String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME = " Propagation of result belonging to community through organization";

	public static final String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID = "authorpid:result";
	public static final String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME = "Propagation of authors pid to result through semantic relations";

	public static final String RELATION_DATASOURCE_ORGANIZATION_REL_CLASS = "isProvidedBy";

	public static final String RELATION_RESULTORGANIZATION_REL_TYPE = "resultOrganization";
	public static final String RELATION_RESULTORGANIZATION_SUBREL_TYPE = "affiliation";
	public static final String RELATION_ORGANIZATION_RESULT_REL_CLASS = "isAuthorInstitutionOf";
	public static final String RELATION_RESULT_ORGANIZATION_REL_CLASS = "hasAuthorInstitution";

	public static final String RELATION_RESULTRESULT_REL_TYPE = "resultResult";

	public static final String RELATION_RESULTPROJECT_REL_TYPE = "resultProject";
	public static final String RELATION_RESULTPROJECT_SUBREL_TYPE = "outcome";
	public static final String RELATION_RESULT_PROJECT_REL_CLASS = "isProducedBy";
	public static final String RELATION_PROJECT_RESULT_REL_CLASS = "produces";

	public static final String RELATION_REPRESENTATIVERESULT_RESULT_CLASS = "merges";

	public static final String PROPAGATION_AUTHOR_PID = "ORCID";

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static Country getCountry(String classid, String classname) {
		Country nc = new Country();
		nc.setClassid(classid);
		nc.setClassname(classname);
		nc.setSchemename(DNET_COUNTRY_SCHEMA);
		nc.setSchemeid(DNET_COUNTRY_SCHEMA);
		nc
			.setDataInfo(
				getDataInfo(
					PROPAGATION_DATA_INFO_TYPE,
					PROPAGATION_COUNTRY_INSTREPO_CLASS_ID,
					PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME));
		return nc;
	}

	public static DataInfo getDataInfo(
		String inference_provenance, String inference_class_id, String inference_class_name) {
		DataInfo di = new DataInfo();
		di.setInferred(true);
		di.setDeletedbyinference(false);
		di.setTrust("0.85");
		di.setInferenceprovenance(inference_provenance);
		di.setProvenanceaction(getQualifier(inference_class_id, inference_class_name));
		return di;
	}

	public static Qualifier getQualifier(String inference_class_id, String inference_class_name) {
		Qualifier pa = new Qualifier();
		pa.setClassid(inference_class_id);
		pa.setClassname(inference_class_name);
		pa.setSchemeid(DNET_SCHEMA_ID);
		pa.setSchemename(DNET_SCHEMA_NAME);
		return pa;
	}

	public static Relation getRelation(
		String source,
		String target,
		String rel_class,
		String rel_type,
		String subrel_type,
		String inference_provenance,
		String inference_class_id,
		String inference_class_name) {
		Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setRelClass(rel_class);
		r.setRelType(rel_type);
		r.setSubRelType(subrel_type);
		r.setDataInfo(getDataInfo(inference_provenance, inference_class_id, inference_class_name));
		return r;
	}

	public static String getConstraintList(String text, List<String> constraints) {
		String ret = " and (" + text + constraints.get(0) + "'";
		for (int i = 1; i < constraints.size(); i++) {
			ret += " OR " + text + constraints.get(i) + "'";
		}
		ret += ")";
		return ret;
	}

	public static void createOutputDirs(String outputPath, FileSystem fs) throws IOException {
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		fs.mkdirs(new Path(outputPath));
	}

	public static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static Boolean isSparkSessionManaged(ArgumentApplicationParser parser) {
		return Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
	}

	public static Boolean isTest(ArgumentApplicationParser parser) {
		return Optional
			.ofNullable(parser.get("isTest"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);
	}

	public static void createCfHbforresult(SparkSession spark) {
		String query;
		query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb "
			+ "FROM ( SELECT id, instance "
			+ "FROM result "
			+ " WHERE datainfo.deletedbyinference = false)  ds "
			+ "LATERAL VIEW EXPLODE(instance) i AS inst";
		org.apache.spark.sql.Dataset<Row> cfhb = spark.sql(query);
		cfhb.createOrReplaceTempView("cfhb");
	}

	public static <R extends Result> org.apache.spark.sql.Dataset<R> readPathEntity(
		SparkSession spark, String inputEntityPath, Class<R> resultClazz) {

		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, resultClazz),
				Encoders.bean(resultClazz));
	}

	public static org.apache.spark.sql.Dataset<Relation> readRelations(
		SparkSession spark, String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, Relation>) value -> OBJECT_MAPPER.readValue(value, Relation.class),
				Encoders.bean(Relation.class));
	}

	public static org.apache.spark.sql.Dataset<ResultCommunityList> readResultCommunityList(
		SparkSession spark, String possibleUpdatesPath) {
		return spark
			.read()
			.textFile(possibleUpdatesPath)
			.map(
				value -> OBJECT_MAPPER.readValue(value, ResultCommunityList.class),
				Encoders.bean(ResultCommunityList.class));
	}
}
