
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.*;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;

public class MergeEntitiesComparator implements Comparator<Oaf> {
	static final List<String> PID_AUTHORITIES = Arrays
		.asList(
			ModelConstants.ARXIV_ID,
			ModelConstants.PUBMED_CENTRAL_ID,
			ModelConstants.EUROPE_PUBMED_CENTRAL_ID,
			ModelConstants.DATACITE_ID,
			ModelConstants.CROSSREF_ID);

	static final List<String> RESULT_TYPES = Arrays
		.asList(
			ModelConstants.ORP_RESULTTYPE_CLASSID,
			ModelConstants.SOFTWARE_RESULTTYPE_CLASSID,
			ModelConstants.DATASET_RESULTTYPE_CLASSID,
			ModelConstants.PUBLICATION_RESULTTYPE_CLASSID);

	public static final Comparator<Oaf> INSTANCE = new MergeEntitiesComparator();

	@Override
	public int compare(Oaf left, Oaf right) {
		if (left == null && right == null)
			return 0;
		if (left == null)
			return -1;
		if (right == null)
			return 1;

		int res = 0;

		// pid authority
		int cfp1 = Optional
			.ofNullable(left.getCollectedfrom())
			.map(
				cf -> cf
					.stream()
					.map(kv -> PID_AUTHORITIES.indexOf(kv.getKey()))
					.max(Integer::compare)
					.orElse(-1))
			.orElse(-1);
		int cfp2 = Optional
			.ofNullable(right.getCollectedfrom())
			.map(
				cf -> cf
					.stream()
					.map(kv -> PID_AUTHORITIES.indexOf(kv.getKey()))
					.max(Integer::compare)
					.orElse(-1))
			.orElse(-1);

		if (cfp1 >= 0 && cfp1 > cfp2) {
			return 1;
		} else if (cfp2 >= 0 && cfp2 > cfp1) {
			return -1;
		}

		// trust
		if (left.getDataInfo() != null && right.getDataInfo() != null) {
			res = left.getDataInfo().getTrust().compareTo(right.getDataInfo().getTrust());
		}

		// result type
		if (res == 0) {
			if (left instanceof Result && right instanceof Result) {
				Result r1 = (Result) left;
				Result r2 = (Result) right;

				if (r1.getResulttype() == null || r1.getResulttype().getClassid() == null) {
					if (r2.getResulttype() != null && r2.getResulttype().getClassid() != null) {
						return -1;
					}
				} else if (r2.getResulttype() == null || r2.getResulttype().getClassid() == null) {
					return 1;
				}

				int rt1 = RESULT_TYPES.indexOf(r1.getResulttype().getClassid());
				int rt2 = RESULT_TYPES.indexOf(r2.getResulttype().getClassid());

				if (rt1 >= 0 && rt1 > rt2) {
					return 1;
				} else if (rt2 >= 0 && rt2 > rt1) {
					return -1;
				}
			}
		}

		// id
		if (res == 0) {
			if (left instanceof OafEntity && right instanceof OafEntity) {
				res = ((OafEntity) left).getId().compareTo(((OafEntity) right).getId());
			}
		}

		return res;
	}

}
