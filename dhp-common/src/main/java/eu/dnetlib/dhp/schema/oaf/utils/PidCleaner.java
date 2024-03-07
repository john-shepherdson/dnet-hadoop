
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Optional;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class PidCleaner {

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

	public static String normalizePidValue(String pidType, String pidValue) {
		String value = Optional
			.ofNullable(pidValue)
			.map(String::trim)
			.orElseThrow(() -> new IllegalArgumentException("PID value cannot be empty"));

		switch (pidType) {

			// TODO add cleaning for more PID types as needed

			// Result
			case "doi":
				return DoiCleaningRule.clean(value);
			case "pmid":
				return PmidCleaningRule.clean(value);
			case "pmc":
				return PmcCleaningRule.clean(value);
			case "handle":
			case "arXiv":
				return value;

			// Organization
			case "GRID":
				return GridCleaningRule.clean(value);
			case "ISNI":
				return ISNICleaningRule.clean(value);
			case "ROR":
				return RorCleaningRule.clean(value);
			case "PIC":
				return PICCleaningRule.clean(value);
			case "FundRef":
				return FundRefCleaningRule.clean(value);
			default:
				return value;
		}
	}

}
