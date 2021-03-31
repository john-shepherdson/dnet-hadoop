
package eu.dnetlib.dhp.oa.provision;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationComparator implements Comparator<Relation> {

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put(ModelConstants.OUTCOME, 0);
		weights.put(ModelConstants.SUPPLEMENT, 1);
		weights.put(ModelConstants.REVIEW, 2);
		weights.put(ModelConstants.CITATION, 3);
		weights.put(ModelConstants.AFFILIATION, 4);
		weights.put(ModelConstants.RELATIONSHIP, 5);
		weights.put(ModelConstants.PUBLICATION_DATASET, 6);
		weights.put(ModelConstants.SIMILARITY, 7);

		weights.put(ModelConstants.PROVISION, 8);
		weights.put(ModelConstants.PARTICIPATION, 9);
		weights.put(ModelConstants.DEDUP, 10);
	}

	private Integer getWeight(Relation o) {
		return Optional.ofNullable(weights.get(o.getSubRelType())).orElse(Integer.MAX_VALUE);
	}

	@Override
	public int compare(Relation o1, Relation o2) {
		return ComparisonChain
			.start()
			.compare(getWeight(o1), getWeight(o2))
			.result();
	}
}
