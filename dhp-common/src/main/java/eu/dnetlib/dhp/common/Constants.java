
package eu.dnetlib.dhp.common;

import com.google.common.collect.Maps;

import java.util.Map;

public class Constants {

	public static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();
	public static final Map<String, String> coarCodeLabelMap = Maps.newHashMap();

	public static final String INFERRED = "Inferred by OpenAIRE";

	public static final String HARVESTED = "Harvested";
	public static final String DEFAULT_TRUST = "0.9";
	public static final String USER_CLAIM = "Linked by user";;

	public static String COAR_ACCESS_RIGHT_SCHEMA = "http://vocabularies.coar-repositories.org/documentation/access_rights/";

	public static String ZENODO_COMMUNITY_PREFIX = "https://zenodo.org/communities/";

	public static String RESEARCH_COMMUNITY = "Research Community";

	public static String RESEARCH_INFRASTRUCTURE = "Research Infrastructure/Initiative";

	public static String ORCID = "orcid";

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

}
