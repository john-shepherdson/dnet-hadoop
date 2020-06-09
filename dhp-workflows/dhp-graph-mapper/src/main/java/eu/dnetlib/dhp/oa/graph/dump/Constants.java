package eu.dnetlib.dhp.oa.graph.dump;

import com.google.common.collect.Maps;

import java.util.Map;

public class Constants {

    public static final Map<String, String> accessRightsCoarMap = Maps.newHashMap();

    static {
        accessRightsCoarMap.put("OPEN", "http://purl.org/coar/access_right/c_abf2");
        accessRightsCoarMap.put("RESTRICTED", "http://purl.org/coar/access_right/c_16ec");
        accessRightsCoarMap.put("OPEN SOURCE", "http://purl.org/coar/access_right/c_abf2");
        accessRightsCoarMap.put("CLOSED", "http://purl.org/coar/access_right/c_14cb //metadataonly for coar");
        accessRightsCoarMap.put("EMBARGO", "http://purl.org/coar/access_right/c_f1cf");
    }
}
