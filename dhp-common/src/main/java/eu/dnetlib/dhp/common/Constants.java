
package eu.dnetlib.dhp.common;

import java.util.Map;

import com.google.common.collect.Maps;

public class Constants {

	public static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();
	public static final Map<String, String> coarCodeLabelMap = Maps.newHashMap();

	public static final String ROR_NS_PREFIX = "ror_________";

	public static final String ROR_OPENAIRE_ID = "10|openaire____::993a7ae7a863813cf95028b50708e222";

	public static final String ROR_DATASOURCE_NAME = "Research Organization Registry (ROR)";

	public static String COAR_ACCESS_RIGHT_SCHEMA = "http://vocabularies.coar-repositories.org/documentation/access_rights/";

	private Constants() {
	}

	static {
		accessRightsCoarMap.put("OPEN", "c_abf2");
		accessRightsCoarMap.put("RESTRICTED", "c_16ec");
		accessRightsCoarMap.put("OPEN SOURCE", "c_abf2");
		accessRightsCoarMap.put("CLOSED", "c_14cb");
		accessRightsCoarMap.put("EMBARGO", "c_f1cf");
	}

	static {
		coarCodeLabelMap.put("c_abf2", "OPEN");
		coarCodeLabelMap.put("c_16ec", "RESTRICTED");
		coarCodeLabelMap.put("c_14cb", "CLOSED");
		coarCodeLabelMap.put("c_f1cf", "EMBARGO");
	}

	public static final String SEQUENCE_FILE_NAME = "/sequence_file";
	public static final String REPORT_FILE_NAME = "/report";
	public static final String MDSTORE_DATA_PATH = "/store";
	public static final String MDSTORE_SIZE_PATH = "/size";

	public static final String COLLECTION_MODE = "collectionMode";
	public static final String METADATA_ENCODING = "metadataEncoding";
	public static final String OOZIE_WF_PATH = "oozieWfPath";
	public static final String DNET_MESSAGE_MGR_URL = "dnetMessageManagerURL";

	public static final String MAX_NUMBER_OF_RETRY = "maxNumberOfRetry";
	public static final String REQUEST_DELAY = "requestDelay";
	public static final String RETRY_DELAY = "retryDelay";
	public static final String CONNECT_TIMEOUT = "connectTimeOut";
	public static final String READ_TIMEOUT = "readTimeOut";
	public static final String FROM_DATE_OVERRIDE = "fromDateOverride";
	public static final String UNTIL_DATE_OVERRIDE = "untilDateOverride";

	public static final String CONTENT_TOTALITEMS = "TotalItems";
	public static final String CONTENT_INVALIDRECORDS = "InvalidRecords";
	public static final String CONTENT_TRANSFORMEDRECORDS = "transformedItems";

	// IETF Draft and used by Repositories like ZENODO , not included in APACHE HTTP java packages
	// see https://ietf-wg-httpapi.github.io/ratelimit-headers/draft-ietf-httpapi-ratelimit-headers.html
	public static final String HTTPHEADER_IETF_DRAFT_RATELIMIT_LIMIT = "X-RateLimit-Limit";
	public static final String HTTPHEADER_IETF_DRAFT_RATELIMIT_REMAINING = "X-RateLimit-Remaining";
	public static final String HTTPHEADER_IETF_DRAFT_RATELIMIT_RESET = "X-RateLimit-Reset";

}
