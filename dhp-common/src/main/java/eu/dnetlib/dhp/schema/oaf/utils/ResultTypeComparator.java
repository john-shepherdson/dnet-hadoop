
package eu.dnetlib.dhp.schema.oaf.utils;

import static eu.dnetlib.dhp.schema.common.ModelConstants.CROSSREF_ID;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;

public class ResultTypeComparator implements Comparator<Result> {

	public static final ResultTypeComparator INSTANCE = new ResultTypeComparator();

	@Override
	public int compare(Result left, Result right) {

		if (left == null && right == null)
			return 0;
		if (left == null)
			return 1;
		if (right == null)
			return -1;

		HashSet<String> lCf = getCollectedFromIds(left);
		HashSet<String> rCf = getCollectedFromIds(right);

		if (lCf.contains(CROSSREF_ID) && !rCf.contains(CROSSREF_ID)) {
			return -1;
		}
		if (!lCf.contains(CROSSREF_ID) && rCf.contains(CROSSREF_ID)) {
			return 1;
		}

		String lClass = left.getResulttype().getClassid();
		String rClass = right.getResulttype().getClassid();

		if (!lClass.equals(rClass)) {
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
		}

		// Else (but unlikely), lexicographical ordering will do.
		return lClass.compareTo(rClass);
	}

	protected HashSet<String> getCollectedFromIds(Result left) {
		return Optional
			.ofNullable(left.getCollectedfrom())
			.map(
				cf -> cf
					.stream()
					.map(KeyValue::getKey)
					.collect(Collectors.toCollection(HashSet::new)))
			.orElse(new HashSet<>());
	}
}
