package eu.dnetlib.dhp.oa.provision.utils;

import static org.apache.commons.lang3.StringUtils.substringAfter;

import com.google.common.collect.Sets;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.Set;

public class GraphMappingUtils {

    public static final String SEPARATOR = "_";

    public static Set<String> authorPidTypes = Sets.newHashSet("orcid", "magidentifier");

    public static String removePrefix(final String s) {
        if (s.contains("|")) return substringAfter(s, "|");
        return s;
    }

    public static String getRelDescriptor(String relType, String subRelType, String relClass) {
        return relType + SEPARATOR + subRelType + SEPARATOR + relClass;
    }
}
