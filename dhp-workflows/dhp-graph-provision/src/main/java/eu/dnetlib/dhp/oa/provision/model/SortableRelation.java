
package eu.dnetlib.dhp.oa.provision.model;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.schema.oaf.Relation;
import java.io.Serializable;
import java.util.Map;

public class SortableRelation extends Relation implements Comparable<Relation>, Serializable {

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put("outcome", 0);
		weights.put("supplement", 1);
		weights.put("publicationDataset", 2);
		weights.put("relationship", 3);
		weights.put("similarity", 4);
		weights.put("affiliation", 5);

		weights.put("provision", 6);
		weights.put("participation", 7);
		weights.put("dedup", 8);
	}

	@Override
	public int compareTo(Relation o) {
		return ComparisonChain
			.start()
			.compare(weights.get(getSubRelType()), weights.get(o.getSubRelType()))
			.compare(getSource(), o.getSource())
			.compare(getTarget(), o.getTarget())
			.result();
	}
}
