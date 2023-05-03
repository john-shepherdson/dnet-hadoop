
package eu.dnetlib.dhp.oa.provision;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationComparator implements Comparator<Relation> {

	private static final Map<Relation.SUBRELTYPE, Integer> weights = Maps.newHashMap();

	
	static {
		weights.put(Relation.SUBRELTYPE.outcome, 0);
		weights.put(Relation.SUBRELTYPE.supplement, 1);
		weights.put(Relation.SUBRELTYPE.review, 2);
		weights.put(Relation.SUBRELTYPE.citation, 3);
		weights.put(Relation.SUBRELTYPE.affiliation, 4);
		//TODO CLAUDIO PLEASE CHECK IF the SUBSTITUTION OF publicationDataset WITH RELATIONSHIPS IS OK
//		weights.put(Relation.SUBRELTYPE.relationship, 5);
		weights.put(Relation.SUBRELTYPE.relationship, 6);
		weights.put(Relation.SUBRELTYPE.similarity, 7);

		weights.put(Relation.SUBRELTYPE.provision, 8);
		weights.put(Relation.SUBRELTYPE.participation, 9);
		weights.put(Relation.SUBRELTYPE.dedup, 10);
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
