
package eu.dnetlib.dhp;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PropagationConstant {

	private PropagationConstant() {
	}

	public static final String DOI = "doi";
	public static final String REF_DOI = ".refs";

	public static final String UPDATE_DATA_INFO_TYPE = "update";
	public static final String UPDATE_SUBJECT_FOS_CLASS_ID = "subject:fos";
	public static final String UPDATE_CLASS_NAME = "Inferred  by OpenAIRE";
	public static final String UPDATE_MEASURE_BIP_CLASS_ID = "measure:bip";

	public static final String FOS_CLASS_ID = "FOS";
	public static final String FOS_CLASS_NAME = "Fields of Science and Technology classification";

	public static final String OPENCITATIONS_CLASSID = "sysimport:crosswalk:opencitations";
	public static final String OPENCITATIONS_CLASSNAME = "Imported from OpenCitations";
	public static final String ID_PREFIX = "50|doi_________::";
	public static final String OC_TRUST = "0.91";

	public final static String NULL = "NULL";

	public static final String INSTITUTIONAL_REPO_TYPE = "institutional";

	public static final String PROPAGATION_DATA_INFO_TYPE = "propagation";

	public static final String TRUE = "true";

	public static final String PROPAGATION_COUNTRY_INSTREPO_CLASS_ID = "country:instrepos";
	public static final String PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME = "Propagation of country to result collected from datasources of type institutional repositories";

	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID = "result:organization:instrepo";
	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME = "Propagation of affiliation to result collected from datasources of type institutional repository";

	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID = "result:organization:semrel";
	public static final String PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME = "Propagation of affiliation to result through semantic relations";

	public static final String PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_ID = "project:organization:semrel";
	public static final String PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_NAME = "Propagation of participation to project through semantic relations";

	public static final String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID = "result:project:semrel";
	public static final String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME = "Propagation of result to project through semantic relation";

	public static final String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID = "result:community:semrel";
	public static final String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME = " Propagation of result belonging to community through semantic relation";

	public static final String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID = "result:community:organization";
	public static final String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME = " Propagation of result belonging to community through organization";

	public static final String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID = "authorpid:result";
	public static final String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME = "Propagation of authors pid to result through semantic relations";

	public static final String ITERATION_ONE = "ExitAtFirstIteration";
	public static final String ITERATION_TWO = "ExitAtSecondIteration";
	public static final String ITERATION_THREE = "ExitAtThirdIteration";
	public static final String ITERATION_FOUR = "ExitAtFourthIteration";
	public static final String ITERATION_FIVE = "ExitAtFifthIteration";
	public static final String ITERATION_NO_PARENT = "ExitAtNoFirstParentReached";

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
					PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME,
					ModelConstants.DNET_PROVENANCE_ACTIONS));
		return nc;
	}

	public static DataInfo getDataInfo(
		String inference_provenance, String inference_class_id, String inference_class_name, String qualifierSchema) {

		return getDataInfo(inference_provenance, inference_class_id, inference_class_name, qualifierSchema, "0.85");
	}

	public static DataInfo getDataInfo(
		String inference_provenance, String inference_class_id, String inference_class_name, String qualifierSchema,
		String trust) {
		return getDataInfo(
			inference_provenance, inference_class_id, inference_class_name, qualifierSchema, trust, true);

	}

	public static DataInfo getDataInfo(
		String inference_provenance, String inference_class_id, String inference_class_name, String qualifierSchema,
		String trust, boolean inferred) {
		DataInfo di = new DataInfo();
		di.setInferred(inferred);
		di.setDeletedbyinference(false);
		di.setTrust(trust);
		di.setInferenceprovenance(inference_provenance);
		di.setProvenanceaction(getQualifier(inference_class_id, inference_class_name, qualifierSchema));
		return di;
	}

	public static Qualifier getQualifier(String inference_class_id, String inference_class_name,
		String qualifierSchema) {
		Qualifier pa = new Qualifier();
		pa.setClassid(inference_class_id);
		pa.setClassname(inference_class_name);
		pa.setSchemeid(qualifierSchema);
		pa.setSchemename(qualifierSchema);
		return pa;
	}

	public static ArrayList<Relation> getOrganizationRelationPair(String orgId,
		String resultId,
		String classID,
		String className

	) {
		ArrayList<Relation> newRelations = new ArrayList();
		newRelations
			.add(
				getRelation(
					orgId,
					resultId,
					ModelConstants.IS_AUTHOR_INSTITUTION_OF,
					ModelConstants.RESULT_ORGANIZATION,
					ModelConstants.AFFILIATION,
					PROPAGATION_DATA_INFO_TYPE,
					classID,
					className));
		newRelations
			.add(
				getRelation(
					resultId,
					orgId,
					ModelConstants.HAS_AUTHOR_INSTITUTION,
					ModelConstants.RESULT_ORGANIZATION,
					ModelConstants.AFFILIATION,
					PROPAGATION_DATA_INFO_TYPE,
					classID,
					className));

		return newRelations;
	}

	public static Relation getRelation(String source, String target, String rel_class) {
		if (ModelConstants.HAS_PARTICIPANT.equals(rel_class)) {
			return getParticipantRelation(source, target, rel_class);
		} else
			return getAffiliationRelation(source, target, rel_class);
	}

	public static Relation getParticipantRelation(
		String source,
		String target,
		String rel_class) {
		return getRelation(
			source, target,
			rel_class,
			ModelConstants.PROJECT_ORGANIZATION,
			ModelConstants.PARTICIPATION,
			PROPAGATION_DATA_INFO_TYPE,
			PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_ID,
			PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_NAME);
	}

	public static Relation getAffiliationRelation(
		String source,
		String target,
		String rel_class) {
		return getRelation(
			source, target,
			rel_class,
			ModelConstants.RESULT_ORGANIZATION,
			ModelConstants.AFFILIATION,
			PROPAGATION_DATA_INFO_TYPE,
			PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
			PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME);
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
		r
			.setDataInfo(
				getDataInfo(
					inference_provenance, inference_class_id, inference_class_name,
					ModelConstants.DNET_PROVENANCE_ACTIONS));
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

		if (HdfsSupport.exists(inputPath, spark.sparkContext().hadoopConfiguration())) {
			return spark
				.read()
				.textFile(inputPath)
				.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
		} else {
			return spark.emptyDataset(Encoders.bean(clazz));
		}
	}

}
