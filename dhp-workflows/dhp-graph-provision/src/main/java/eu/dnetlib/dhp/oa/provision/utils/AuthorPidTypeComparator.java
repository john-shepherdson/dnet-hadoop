
package eu.dnetlib.dhp.oa.provision.utils;

import java.util.Comparator;
import java.util.Optional;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class AuthorPidTypeComparator implements Comparator<StructuredProperty> {

	@Override
	public int compare(StructuredProperty left, StructuredProperty right) {

		String lClass = Optional
			.ofNullable(left)
			.map(StructuredProperty::getQualifier)
			.map(Qualifier::getClassid)
			.orElse(null);

		String rClass = Optional
			.ofNullable(right)
			.map(StructuredProperty::getQualifier)
			.map(Qualifier::getClassid)
			.orElse(null);

		if (lClass == null && rClass == null)
			return 0;
		if (lClass == null)
			return 1;
		if (rClass == null)
			return -1;

		if (lClass.equals(rClass))
			return 0;

		if (lClass.equals(ModelConstants.ORCID))
			return -1;
		if (rClass.equals(ModelConstants.ORCID))
			return 1;

		if (lClass.equals(ModelConstants.ORCID_PENDING))
			return -1;
		if (rClass.equals(ModelConstants.ORCID_PENDING))
			return 1;

		return 0;
	}

}
