
package eu.dnetlib.dhp;

import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class PropagationConstant {
	public static final String INSTITUTIONAL_REPO_TYPE = "pubsrepository::institutional";

	public static final String PROPAGATION_DATA_INFO_TYPE = "propagation";

	public static final String TRUE = "true";

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

	public static final String PROPAGATION_AUTHOR_PID_CLASSID = "orcid_pending";
	public static final String ORCID = "orcid";
	public static final String PROPAGATION_AUTHOR_PID_CLASSNAME = "Open Researcher and Contributor ID";


	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String cfHbforResultQuery = "select distinct r.id, inst.collectedfrom.key cf, inst.hostedby.key hb "
		+
		"from result r " +
		"lateral view explode(instance) i as inst " +
		"where r.datainfo.deletedbyinference=false";

	public static Country getCountry(String classid, String classname) {
		Country nc = new Country();
		nc.setClassid(classid);
		nc.setClassname(classname);
		nc.setSchemename(ModelConstants.DNET_COUNTRY_TYPE);
		nc.setSchemeid(ModelConstants.DNET_COUNTRY_TYPE);
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
		pa.setSchemeid(ModelConstants.DNET_PID_TYPES);
		pa.setSchemename(ModelConstants.DNET_PID_TYPES);
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
		String ret = " and (" + text + constraints.get(0).toLowerCase() + "'";
		for (int i = 1; i < constraints.size(); i++) {
			ret += " OR " + text + constraints.get(i).toLowerCase() + "'";
		}
		ret += ")";
		return ret;
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

	public static void createCfHbforResult(SparkSession spark) {
		org.apache.spark.sql.Dataset<Row> cfhb = spark.sql(cfHbforResultQuery);
		cfhb.createOrReplaceTempView("cfhb");
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
