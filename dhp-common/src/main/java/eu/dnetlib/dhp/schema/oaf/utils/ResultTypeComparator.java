
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

		Result.RESULTTYPE lType = left.getResulttype();
		Result.RESULTTYPE rType = right.getResulttype();

		if (lType.equals(rType))
			return 0;

		if (lType.equals(Result.RESULTTYPE.publication))
			return -1;
		if (rType.equals(Result.RESULTTYPE.publication))
			return 1;

		if (lType.equals(Result.RESULTTYPE.dataset))
			return -1;
		if (rType.equals(Result.RESULTTYPE.dataset))
			return 1;

		if (lType.equals(Result.RESULTTYPE.software))
			return -1;
		if (rType.equals(Result.RESULTTYPE.software))
			return 1;

		if (lType.equals(Result.RESULTTYPE.otherresearchproduct))
			return -1;
		if (rType.equals(Result.RESULTTYPE.otherresearchproduct))
			return 1;

		// Else (but unlikely), lexicographical ordering will do.
		return lType.compareTo(rType);
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
