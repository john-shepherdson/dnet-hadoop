
package eu.dnetlib.dhp.oa.graph.dump;

import java.util.Map;

import com.google.common.collect.Maps;

public class Constants {

	public static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();
	public static final Map<String, String> coarCodeLabelMap = Maps.newHashMap();

	public static final String INFERRED = "Inferred by OpenAIRE";

	public static final String HARVESTED = "Harvested";
	public static final String DEFAULT_TRUST = "0.9";
	public static final String USER_CLAIM = "Linked by user";

	public static String COAR_ACCESS_RIGHT_SCHEMA = "http://vocabularies.coar-repositories.org/documentation/access_rights/";

	public static String ZENODO_COMMUNITY_PREFIX = "https://zenodo.org/communities/";

	public static String RESEARCH_COMMUNITY = "Research Community";

	public static String RESEARCH_INFRASTRUCTURE = "Research Infrastructure/Initiative";

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

	public enum DUMPTYPE {
		COMPLETE("complete"), COMMUNITY("community"), FUNDER("funder");

		private final String type;

		DUMPTYPE(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}
	}
}
