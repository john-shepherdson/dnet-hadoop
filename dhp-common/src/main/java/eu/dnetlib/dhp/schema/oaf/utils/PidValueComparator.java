
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;
import java.util.Optional;

import eu.dnetlib.dhp.oa.graph.clean.CleaningFunctions;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class PidValueComparator implements Comparator<StructuredProperty> {

	@Override
	public int compare(StructuredProperty left, StructuredProperty right) {

		if (left == null && right == null)
			return 0;
		if (left == null)
			return 1;
		if (right == null)
			return -1;

		StructuredProperty l = CleaningFunctions.normalizePidValue(left);
		StructuredProperty r = CleaningFunctions.normalizePidValue(right);

		return Optional
			.ofNullable(l.getValue())
			.map(
				lv -> Optional
					.ofNullable(r.getValue())
					.map(rv -> lv.compareTo(rv))
					.orElse(-1))
			.orElse(1);
	}
}
