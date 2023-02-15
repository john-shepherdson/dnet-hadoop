
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import lombok.val;

public class CleaningFunctions {

	public static final String DOI_PREFIX_REGEX = "(^10\\.|\\/10\\.)";

	private static final String ALL_SPACES_REGEX = "(?:\\n|\\r|\\t|\\s)";
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

	/**
	 * Utility method that normalises PID values on a per-type basis.
	 * @param pid the PID whose value will be normalised.
	 * @return the PID containing the normalised value.
	 */
	public static StructuredProperty normalizePidValue(StructuredProperty pid) {
		pid
			.setValue(
				normalizePidValue(
					pid.getQualifier().getClassid(),
					pid.getValue()));

		return pid;
	}

	/**
	 * This utility was moved from DOIBoost,
	 * it implements a better cleaning of DOI.
	 * In case of wrong DOI it raises an illegalArgumentException
	 * @param input DOI
	 * @return normalized DOI
	 */
	private static String normalizeDOI(final String input) {
		if (input == null)
			throw new IllegalArgumentException("PID value cannot be empty");
		final String replaced = input
			.replaceAll(ALL_SPACES_REGEX, "")
			.toLowerCase()
			.replaceFirst(DOI_PREFIX_REGEX, DOI_PREFIX);
		if (StringUtils.isEmpty(replaced.trim()))
			throw new IllegalArgumentException("PID value normalized return empty string");
		if (!replaced.contains("10."))
			throw new IllegalArgumentException("DOI Must starts with 10.");
		return replaced.substring(replaced.indexOf("10."));
	}

	public static String normalizePidValue(String pidType, String pidValue) {
		String value = Optional
			.ofNullable(pidValue)
			.map(String::trim)
			.orElseThrow(() -> new IllegalArgumentException("PID value cannot be empty"));

		switch (pidType) {

			// TODO add cleaning for more PID types as needed
			case ModelConstants.DOI:
				return normalizeDOI(value.toLowerCase());
		}
		return value;
	}

}
