
package eu.dnetlib.dhp.oa.provision.utils;

import static org.apache.commons.lang3.StringUtils.substringAfter;

import java.util.Set;

import com.google.common.collect.Sets;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class GraphMappingUtils {

	public static final String SEPARATOR = "_";

	public static final Set<String> authorPidTypes = Sets
		.newHashSet(
			ModelConstants.ORCID, ModelConstants.ORCID_PENDING, "magidentifier");

	private GraphMappingUtils() {
	}

	public static String removePrefix(final String s) {
		if (s.contains("|"))
			return substringAfter(s, "|");
		return s;
	}

}
