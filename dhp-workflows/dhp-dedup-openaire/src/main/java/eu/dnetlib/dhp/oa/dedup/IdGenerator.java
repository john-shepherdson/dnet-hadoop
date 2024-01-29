
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.utils.DHPUtils.md5;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class IdGenerator implements Serializable {

	// pick the best pid from the list (consider date and pidtype)
	public static <T extends OafEntity> String generate(List<? extends Identifier> pids, String defaultID) {
		if (pids == null || pids.isEmpty())
			return defaultID;

		return generateId(pids);
	}

	private static String generateId(List<? extends Identifier> pids) {
		Identifier bp = pids
			.stream()
			.min(Identifier::compareTo)
			.orElseThrow(() -> new IllegalStateException("unable to generate id"));

		return generate(bp.getOriginalID());
	}

	public static String generate(String originalId) {
		String prefix = substringBefore(originalId, "|");
		String ns = substringBefore(substringAfter(originalId, "|"), "::");
		String suffix = substringAfter(originalId, "::");

		final String pidType = substringBefore(ns, "_");
		if (PidType.isValid(pidType)) {
			return prefix + "|" + dedupify(ns) + "::" + suffix;
		} else {
			return prefix + "|dedup_wf_002::" + md5(originalId); // hash the whole originalId to avoid collisions
		}
	}

	private static String dedupify(String ns) {
		StringBuilder prefix;
		if (PidType.valueOf(substringBefore(ns, "_")) == PidType.openorgs) {
			prefix = new StringBuilder(substringBefore(ns, "_"));
		} else {
			prefix = new StringBuilder(substringBefore(ns, "_")).append("_dedup");
		}

		while (prefix.length() < 12) {
			prefix.append("_");
		}
		return prefix.substring(0, 12);
	}
}
