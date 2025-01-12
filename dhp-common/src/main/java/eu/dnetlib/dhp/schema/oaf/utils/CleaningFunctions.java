
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class CleaningFunctions {

	public static final String DOI_PREFIX_REGEX = "(^10\\.|\\/10\\.)";
	public static final String DOI_PREFIX = "10.";

	public static final Set<String> PID_BLACKLIST = new HashSet<>();

	static {
		PID_BLACKLIST.add("none");
		PID_BLACKLIST.add("na");
	}

	public CleaningFunctions() {
	}

	/**
	 * Utility method that filter PID values on a per-type basis.
	 * @param s the PID whose value will be checked.
	 * @return false if the pid matches the filter criteria, true otherwise.
	 */
	public static boolean pidFilter(StructuredProperty s) {
		final String pidValue = s.getValue();
		if (Objects.isNull(s.getQualifier()) ||
			StringUtils.isBlank(pidValue) ||
			StringUtils.isBlank(pidValue.replaceAll("(?:\\n|\\r|\\t|\\s)", ""))) {
			return false;
		}
		if (CleaningFunctions.PID_BLACKLIST.contains(pidValue)) {
			return false;
		}
		return !PidBlacklistProvider.getBlacklist(s.getQualifier().getClassid()).contains(pidValue);
	}

}
