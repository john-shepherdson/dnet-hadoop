
package eu.dnetlib.dhp.oa.graph.dump;

import java.util.Map;

import com.google.common.collect.Maps;

public class Constants {

	public static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();
	public static final Map<String, String> coarCodeLabelMap = Maps.newHashMap();

	public static String COAR_ACCESS_RIGHT_SCHEMA = "http://vocabularies.coar-repositories.org/documentation/access_rights/";

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
