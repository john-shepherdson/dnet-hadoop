
package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class ModelConstants {

	public static final String DNET_RESULT_TYPOLOGIES = "dnet:result_typologies";
	public static final String DNET_PUBLICATION_RESOURCE = "dnet:publication_resource";
	public static final String DNET_ACCESS_MODES = "dnet:access_modes";
	public static final String DNET_LANGUAGES = "dnet:languages";
	public static final String DNET_PID_TYPES = "dnet:pid_types";
	public static final String DNET_DATA_CITE_DATE = "dnet:dataCite_date";
	public static final String DNET_DATA_CITE_RESOURCE = "dnet:dataCite_resource";
	public static final String DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";

	public static final String SYSIMPORT_CROSSWALK_REPOSITORY = "sysimport:crosswalk:repository";
	public static final String SYSIMPORT_CROSSWALK_ENTITYREGISTRY = "sysimport:crosswalk:entityregistry";
	public static final String USER_CLAIM = "user:claim";

	public static final String DATASET_RESULTTYPE_CLASSID = "dataset";
	public static final String PUBLICATION_RESULTTYPE_CLASSID = "publication";
	public static final String SOFTWARE_RESULTTYPE_CLASSID = "software";
	public static final String ORP_RESULTTYPE_CLASSID = "other";

	public static final String RESULT_RESULT = "resultResult";
	public static final String PUBLICATION_DATASET = "publicationDataset";
	public static final String IS_RELATED_TO = "isRelatedTo";
	public static final String SUPPLEMENT = "supplement";
	public static final String IS_SUPPLEMENT_TO = "isSupplementTo";
	public static final String IS_SUPPLEMENTED_BY = "isSupplementedBy";
	public static final String PART = "part";
	public static final String IS_PART_OF = "IsPartOf";
	public static final String HAS_PARTS = "HasParts";
	public static final String RELATIONSHIP = "relationship";

	public static final String RESULT_PROJECT = "resultProject";
	public static final String OUTCOME = "outcome";
	public static final String IS_PRODUCED_BY = "isProducedBy";
	public static final String PRODUCES = "produces";

	public static final String DATASOURCE_ORGANIZATION = "datasourceOrganization";
	public static final String PROVISION = "provision";
	public static final String IS_PROVIDED_BY = "isProvidedBy";
	public static final String PROVIDES = "provides";

	public static final String PROJECT_ORGANIZATION = "projectOrganization";
	public static final String PARTICIPATION = "participation";
	public static final String HAS_PARTICIPANT = "hasParticipant";
	public static final String IS_PARTICIPANT = "isParticipant";

	public static final String UNKNOWN = "UNKNOWN";
	public static final String NOT_AVAILABLE = "not available";

	public static final Qualifier PUBLICATION_DEFAULT_RESULTTYPE = qualifier(
		PUBLICATION_RESULTTYPE_CLASSID, PUBLICATION_RESULTTYPE_CLASSID,
		DNET_RESULT_TYPOLOGIES, DNET_RESULT_TYPOLOGIES);

	public static final Qualifier DATASET_DEFAULT_RESULTTYPE = qualifier(
		DATASET_RESULTTYPE_CLASSID, DATASET_RESULTTYPE_CLASSID,
		DNET_RESULT_TYPOLOGIES, DNET_RESULT_TYPOLOGIES);

	public static final Qualifier SOFTWARE_DEFAULT_RESULTTYPE = qualifier(
		SOFTWARE_RESULTTYPE_CLASSID, SOFTWARE_RESULTTYPE_CLASSID,
		DNET_RESULT_TYPOLOGIES, DNET_RESULT_TYPOLOGIES);

	public static final Qualifier ORP_DEFAULT_RESULTTYPE = qualifier(
		ORP_RESULTTYPE_CLASSID, ORP_RESULTTYPE_CLASSID,
		DNET_RESULT_TYPOLOGIES, DNET_RESULT_TYPOLOGIES);

	public static final Qualifier REPOSITORY_PROVENANCE_ACTIONS = qualifier(
		SYSIMPORT_CROSSWALK_REPOSITORY, SYSIMPORT_CROSSWALK_REPOSITORY,
		DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS);

	public static final Qualifier ENTITYREGISTRY_PROVENANCE_ACTION = qualifier(
		SYSIMPORT_CROSSWALK_ENTITYREGISTRY, SYSIMPORT_CROSSWALK_ENTITYREGISTRY,
		DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS);

	private static Qualifier qualifier(
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename) {
		final Qualifier q = new Qualifier();
		q.setClassid(classid);
		q.setClassname(classname);
		q.setSchemeid(schemeid);
		q.setSchemename(schemename);
		return q;
	}
}
