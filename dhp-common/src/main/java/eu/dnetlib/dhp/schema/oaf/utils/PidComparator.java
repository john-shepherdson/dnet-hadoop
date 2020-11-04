
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class PidComparator<T extends OafEntity> implements Comparator<StructuredProperty> {

	private T entity;

	public PidComparator(T entity) {
		this.entity = entity;
	}

	@Override
	public int compare(StructuredProperty left, StructuredProperty right) {

		if (left == null && right == null)
			return 0;
		if (left == null)
			return 1;
		if (right == null)
			return -1;

		PidType lClass = PidType.valueOf(left.getQualifier().getClassid());
		PidType rClass = PidType.valueOf(right.getQualifier().getClassid());

		if (lClass.equals(rClass))
			return 0;

		if (ModelSupport.isSubClass(entity, Result.class)) {
			return compareResultPids(lClass, rClass);
		}
		if (ModelSupport.isSubClass(entity, Organization.class)) {
			return compareOrganizationtPids(lClass, rClass);
		}

		// Else (but unlikely), lexicographical ordering will do.
		return lClass.compareTo(rClass);
	}

	private int compareResultPids(PidType lClass, PidType rClass) {
		return new ResultPidComparator().compare(lClass, rClass);
	}

	private int compareOrganizationtPids(PidType lClass, PidType rClass) {
		return new OrganizationPidComparator().compare(lClass, rClass);
	}
}
