
package eu.dnetlib.dhp.oa.graph.dump;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class Constants {

	protected static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();
	protected static final Map<String, String> coarCodeLabelMap = Maps.newHashMap();

	public static final String INFERRED = "Inferred by OpenAIRE";
	public static final String CABF2 = "c_abf2";

	public static final String HARVESTED = "Harvested";
	public static final String DEFAULT_TRUST = "0.9";
	public static final String USER_CLAIM = "Linked by user";

	public static final String COAR_ACCESS_RIGHT_SCHEMA = "http://vocabularies.coar-repositories.org/documentation/access_rights/";

	public static final String ZENODO_COMMUNITY_PREFIX = "https://zenodo.org/communities/";

	public static final String RESEARCH_COMMUNITY = "Research Community";

	public static final String RESEARCH_INFRASTRUCTURE = "Research Infrastructure/Initiative";

	static {
		accessRightsCoarMap.put(ModelConstants.ACCESS_RIGHT_OPEN, CABF2);
		accessRightsCoarMap.put("RESTRICTED", "c_16ec");
		accessRightsCoarMap.put("OPEN SOURCE", CABF2);
		accessRightsCoarMap.put(ModelConstants.ACCESS_RIGHT_CLOSED, "c_14cb");
		accessRightsCoarMap.put(ModelConstants.ACCESS_RIGHT_EMBARGO, "c_f1cf");
	}

	static {
		coarCodeLabelMap.put(CABF2, ModelConstants.ACCESS_RIGHT_OPEN);
		coarCodeLabelMap.put("c_16ec", "RESTRICTED");
		coarCodeLabelMap.put("c_14cb", ModelConstants.ACCESS_RIGHT_CLOSED);
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
