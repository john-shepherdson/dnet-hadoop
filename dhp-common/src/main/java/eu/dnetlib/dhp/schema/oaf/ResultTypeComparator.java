
package eu.dnetlib.dhp.schema.oaf;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class ResultTypeComparator implements Comparator<Result> {

	@Override
	public int compare(Result left, Result right) {

		if (left == null && right == null)
			return 0;
		if (left == null)
			return 1;
		if (right == null)
			return -1;

		String lClass = left.getResulttype().getClassid();
		String rClass = right.getResulttype().getClassid();

		if (lClass.equals(rClass))
			return 0;

		if (lClass.equals(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID))
			return 1;

		if (lClass.equals(ModelConstants.DATASET_RESULTTYPE_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.DATASET_RESULTTYPE_CLASSID))
			return 1;

		if (lClass.equals(ModelConstants.SOFTWARE_RESULTTYPE_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.SOFTWARE_RESULTTYPE_CLASSID))
			return 1;

		if (lClass.equals(ModelConstants.ORP_RESULTTYPE_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.ORP_RESULTTYPE_CLASSID))
			return 1;

		// Else (but unlikely), lexicographical ordering will do.
		return lClass.compareTo(rClass);
	}
}
