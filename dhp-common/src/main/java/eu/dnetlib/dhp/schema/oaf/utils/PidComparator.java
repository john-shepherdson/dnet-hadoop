
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

		String lClass = left.getQualifier().getClassid();
		String rClass = right.getQualifier().getClassid();

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

	private int compareResultPids(String lClass, String rClass) {
		if (lClass.equals("doi"))
			return -1;
		if (rClass.equals("doi"))
			return 1;

		if (lClass.equals("pmid"))
			return -1;
		if (rClass.equals("pmid"))
			return 1;

		if (lClass.equals("pmc"))
			return -1;
		if (rClass.equals("pmc"))
			return 1;

		return 0;
	}

	private int compareOrganizationtPids(String lClass, String rClass) {
		if (lClass.equals("GRID"))
			return -1;
		if (rClass.equals("GRID"))
			return 1;

		if (lClass.equals("mag_id"))
			return -1;
		if (rClass.equals("mag_id"))
			return 1;

		if (lClass.equals("urn"))
			return -1;
		if (rClass.equals("urn"))
			return 1;

		return 0;
	}
}
