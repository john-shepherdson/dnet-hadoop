
package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.AccessRight;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class ModelConstants {

	private ModelConstants() {
	}

	public static final String DOI = "doi";
	public static final String ORCID = "orcid";
	public static final String ORCID_PENDING = "orcid_pending";
	public static final String ORCID_CLASSNAME = "Open Researcher and Contributor ID";
	public static final String ORCID_DS = ORCID.toUpperCase();

	public static final String CROSSREF_ID = "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2";

	public static final String CROSSREF_NAME = "Crossref";
	public static final String DATACITE_ID = "10|openaire____::9e3be59865b2c1c335d32dae2fe7b254";

	public static final String ZENODO_OD_ID = "10|opendoar____::358aee4cc897452c00244351e4d91f69";
	public static final String ZENODO_R3_ID = "10|re3data_____::7b0ad08687b2c960d5aeef06f811d5e6";

	public static final String EUROPE_PUBMED_CENTRAL_ID = "10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c";
	public static final String PUBMED_CENTRAL_ID = "10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357";
	public static final String ARXIV_ID = "10|opendoar____::6f4922f45568161a8cdf4ad2299f6d23";
	public static final String ROHUB_ID = "10|fairsharing_::1b69ebedb522700034547abc5652ffac";

	public static final String OPENORGS_NAME = "OpenOrgs Database";

	public static final String OPENOCITATIONS_NAME = "OpenCitations";
	public static final String OPENOCITATIONS_ID = "10|openaire____::c06df618c5de1c786535ccf3f8b7b059";

	public static final String OPEN_APC_NAME = "OpenAPC Global Initiative";
	public static final String OPEN_APC_ID = "10|apc_________::e2b1600b229fc30663c8a1f662debddf";

	// VOCABULARY VALUE
	public static final String ACCESS_RIGHT_OPEN = "OPEN";
	public static final String ACCESS_RIGHT_EMBARGO = "EMBARGO";
	public static final String ACCESS_RIGHT_CLOSED = "CLOSED";

	public static final String DNET_SUBJECT_KEYWORD = "keyword";

	public static final String DNET_SUBJECT_FOS_CLASSID = "FOS";

	public static final String DNET_SUBJECT_FOS_CLASSNAME = "Fields of Science and Technology classification";

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
	public static final String DNET_PROGRAMMING_LANGUAGES = "dnet:programming_languages";
	public static final String DNET_EXTERNAL_REFERENCE_TYPE = "dnet:externalReference_typologies";
	public static final String DNET_RELATION_RELTYPE = "dnet:relation_relType";
	public static final String DNET_RELATION_SUBRELTYPE = "dnet:relation_subRelType";
	public static final String DNET_RELATION_RELCLASS = "dnet:relation_relClass";

	public static final String PEER_REVIEWED_CLASSNAME = "nonPeerReviewed";
	public static final String NON_PEER_REVIEWED_CLASSNAME = "nonPeerReviewed";
	public static final String PEER_REVIEWED_CLASSID = "0001";
	public static final String NON_PEER_REVIEWED_CLASSID = "0002";

	public static final String SYSIMPORT_CROSSWALK_REPOSITORY = "sysimport:crosswalk:repository";
	public static final String SYSIMPORT_CROSSWALK_ENTITYREGISTRY = "sysimport:crosswalk:entityregistry";
	public static final String SYSIMPORT_ACTIONSET = "sysimport:actionset";
	public static final String SYSIMPORT_ORCID_NO_DOI = "sysimport:actionset:orcidworks-no-doi";

	public static final String USER_CLAIM = "user:claim";
	public static final String HARVESTED = "Harvested";

	public static final String PROVENANCE_DEDUP = "sysimport:dedup";
	public static final String PROVENANCE_ENRICH = "sysimport:enrich";

	public static final Qualifier PROVENANCE_ACTION_SET_QUALIFIER = qualifier(
		SYSIMPORT_ACTIONSET, SYSIMPORT_ACTIONSET, DNET_PROVENANCE_ACTIONS);


	public static final String UNKNOWN = "UNKNOWN";
	public static final String NOT_AVAILABLE = "not available";

	public static final Qualifier REPOSITORY_PROVENANCE_ACTIONS = qualifier(
		SYSIMPORT_CROSSWALK_REPOSITORY, SYSIMPORT_CROSSWALK_REPOSITORY,
		DNET_PROVENANCE_ACTIONS);

	public static final Qualifier ENTITYREGISTRY_PROVENANCE_ACTION = qualifier(
		SYSIMPORT_CROSSWALK_ENTITYREGISTRY, SYSIMPORT_CROSSWALK_ENTITYREGISTRY,
		DNET_PROVENANCE_ACTIONS);

	public static final String UNKNOWN_REPOSITORY_ORIGINALID = "openaire____::1256f046-bf1f-4afc-8b47-d0b147148b18";
	public static final KeyValue UNKNOWN_REPOSITORY = keyValue(
		"10|openaire____::55045bd2a65019fd8e6741a755395c8c", "Unknown Repository");

	public static final Qualifier UNKNOWN_COUNTRY = qualifier(UNKNOWN, "Unknown", DNET_COUNTRY_TYPE);

	public static final Qualifier MAIN_TITLE_QUALIFIER = qualifier(
		"main title", "main title", DNET_DATACITE_TITLE);

	public static final Qualifier ALTERNATIVE_TITLE_QUALIFIER = qualifier(
		"alternative title", "alternative title", DNET_DATACITE_TITLE);

	public static final Qualifier SUBTITLE_QUALIFIER = qualifier("subtitle", "subtitle", DNET_DATACITE_TITLE);

	public static final AccessRight OPEN_ACCESS_RIGHT() {

		final AccessRight result = new AccessRight();
		result.setClassid(ACCESS_RIGHT_OPEN);
		result.setClassid(ACCESS_RIGHT_OPEN);
		result.setSchemeid(ModelConstants.DNET_ACCESS_MODES);
		return result;
	}

	public static final AccessRight RESTRICTED_ACCESS_RIGHT() {
		final AccessRight result = new AccessRight();
		result.setClassid("RESTRICTED");
		result.setClassname("Restricted");
		result.setSchemeid(ModelConstants.DNET_ACCESS_MODES);
		return result;
	}

	public static final AccessRight UNKNOWN_ACCESS_RIGHT() {
		return OafMapperUtils
			.accessRight(
				ModelConstants.UNKNOWN,
				ModelConstants.NOT_AVAILABLE,
				ModelConstants.DNET_ACCESS_MODES);
	}

	public static final AccessRight EMBARGOED_ACCESS_RIGHT() {
		return OafMapperUtils
			.accessRight(
				ACCESS_RIGHT_EMBARGO,
				ACCESS_RIGHT_EMBARGO,
				DNET_ACCESS_MODES);
	}

	public static final AccessRight CLOSED_ACCESS_RIGHT() {
		return OafMapperUtils
			.accessRight(
				ACCESS_RIGHT_CLOSED,
				"Closed Access",
				ModelConstants.DNET_ACCESS_MODES);
	}

	private static Qualifier qualifier(
		final String classid,
		final String classname,
		final String schemeid) {
		final Qualifier q = new Qualifier();
		q.setClassid(classid);
		q.setClassname(classname);
		q.setSchemeid(schemeid);
		return q;
	}

	private static KeyValue keyValue(final String key, final String value) {
		final KeyValue kv = new KeyValue();
		kv.setKey(key);
		kv.setValue(value);
		return kv;
	}
}
