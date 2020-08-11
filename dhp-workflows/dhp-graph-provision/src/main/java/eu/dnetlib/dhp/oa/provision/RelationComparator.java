
package eu.dnetlib.dhp.oa.provision;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationComparator implements Comparator<Relation> {

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put("outcome", 0);
		weights.put("supplement", 1);
		weights.put("review", 2);
		weights.put("citation", 3);
		weights.put("affiliation", 4);
		weights.put("relationship", 5);
		weights.put("publicationDataset", 6);
		weights.put("similarity", 7);

		weights.put("provision", 8);
		weights.put("participation", 9);
		weights.put("dedup", 10);
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
