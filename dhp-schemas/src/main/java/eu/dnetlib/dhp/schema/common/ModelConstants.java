
package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class ModelConstants {

	public static final String ORCID = "orcid";
	public static final String ORCID_PENDING = "orcid_pending";
	public static final String ORCID_CLASSNAME = "Open Researcher and Contributor ID";

	public static final String CROSSREF_ID = "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2";
	public static final String DATACITE_ID = "10|openaire____::9e3be59865b2c1c335d32dae2fe7b254";

	public static final String EUROPE_PUBMED_CENTRAL_ID = "10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c";
	public static final String PUBMED_CENTRAL_ID = "10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357";
	public static final String ARXIV_ID = "10|opendoar____::6f4922f45568161a8cdf4ad2299f6d23";

	// VOCABULARY VALUE
	public static final String ACCESS_RIGHT_OPEN = "OPEN";

	public static final String DNET_SUBJECT_TYPOLOGIES = "dnet:subject_classification_typologies";
	public static final String DNET_RESULT_TYPOLOGIES = "dnet:result_typologies";
	public static final String DNET_PUBLICATION_RESOURCE = "dnet:publication_resource";
	public static final String DNET_ACCESS_MODES = "dnet:access_modes";
	public static final String DNET_LANGUAGES = "dnet:languages";
	public static final String DNET_PID_TYPES = "dnet:pid_types";
	public static final String DNET_DATACITE_DATE = "dnet:dataCite_date";
	public static final String DNET_DATACITE_TITLE = "dnet:dataCite_title";
	public static final String DNET_DATA_CITE_RESOURCE = "dnet:dataCite_resource";
	public static final String DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";
	public static final String DNET_COUNTRY_TYPE = "dnet:countries";
	public static final String DNET_REVIEW_LEVELS = "dnet:review_levels";

	public static final String SYSIMPORT_CROSSWALK_REPOSITORY = "sysimport:crosswalk:repository";
	public static final String SYSIMPORT_CROSSWALK_ENTITYREGISTRY = "sysimport:crosswalk:entityregistry";
	public static final String USER_CLAIM = "user:claim";

	public static final String DATASET_RESULTTYPE_CLASSID = "dataset";
	public static final String PUBLICATION_RESULTTYPE_CLASSID = "publication";
	public static final String SOFTWARE_RESULTTYPE_CLASSID = "software";
	public static final String ORP_RESULTTYPE_CLASSID = "other";

	public static final String RESULT_RESULT = "resultResult";
	/**
	 * @deprecated Use {@link ModelConstants#RELATIONSHIP} instead.
	 */
	@Deprecated
	public static final String PUBLICATION_DATASET = "publicationDataset";
	public static final String IS_RELATED_TO = "isRelatedTo";
	public static final String SUPPLEMENT = "supplement";
	public static final String IS_SUPPLEMENT_TO = "isSupplementTo";
	public static final String IS_SUPPLEMENTED_BY = "isSupplementedBy";
	public static final String PART = "part";
	public static final String IS_PART_OF = "isPartOf";
	public static final String HAS_PARTS = "hasParts";
	public static final String RELATIONSHIP = "relationship";
	public static final String CITATION = "citation";
	public static final String CITES = "cites";
	public static final String IS_CITED_BY = "isCitedBy";
	public static final String REVIEW = "review";
	public static final String REVIEWS = "reviews";
	public static final String IS_REVIEWED_BY = "isReviewedBy";

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

	public static final String RESULT_ORGANIZATION = "resultOrganization";
	public static final String AFFILIATION = "affiliation";
	public static final String IS_AUTHOR_INSTITUTION_OF = "isAuthorInstitutionOf";
	public static final String HAS_AUTHOR_INSTITUTION = "hasAuthorInstitution";

	public static final String MERGES = "merges";

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

	public static final KeyValue UNKNOWN_REPOSITORY = keyValue(
		"10|openaire____::55045bd2a65019fd8e6741a755395c8c", "Unknown Repository");

	public static final Qualifier UNKNOWN_COUNTRY = qualifier(UNKNOWN, "Unknown", DNET_COUNTRY_TYPE, DNET_COUNTRY_TYPE);

	public static final Qualifier MAIN_TITLE_QUALIFIER = qualifier(
		"main title", "main title", DNET_DATACITE_TITLE, DNET_DATACITE_TITLE);

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

	private static KeyValue keyValue(String key, String value) {
		KeyValue kv = new KeyValue();
		kv.setKey(key);
		kv.setValue(value);
		kv.setDataInfo(new DataInfo());
		return kv;
	}
}
