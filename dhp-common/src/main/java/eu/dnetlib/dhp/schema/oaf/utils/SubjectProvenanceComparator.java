
package eu.dnetlib.dhp.schema.oaf.utils;

import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.getProvenance;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.Subject;

public class SubjectProvenanceComparator implements Comparator<Subject> {

	@Override
	public int compare(Subject left, Subject right) {

		String lProv = getProvenance(left.getDataInfo());
		String rProv = getProvenance(right.getDataInfo());

		if (isBlank(lProv) && isBlank(rProv))
			return 0;
		if (isBlank(lProv))
			return 1;
		if (isBlank(rProv))
			return -1;
		if (lProv.equals(rProv))
			return 0;
		if (lProv.toLowerCase().contains("crosswalk"))
			return -1;
		if (rProv.toLowerCase().contains("crosswalk"))
			return 1;
		if (lProv.toLowerCase().contains("user"))
			return -1;
		if (rProv.toLowerCase().contains("user"))
			return 1;
		if (lProv.toLowerCase().contains("propagation"))
			return -1;
		if (rProv.toLowerCase().contains("propagation"))
			return 1;
		if (lProv.toLowerCase().contains("iis"))
			return -1;
		if (rProv.toLowerCase().contains("iis"))
			return 1;

		return 0;
	}
}
