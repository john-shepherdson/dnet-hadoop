
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class SortableRelationKey implements Comparable<SortableRelationKey>, Serializable {

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put("outcome", 0);
		weights.put("supplement", 1);
		weights.put("affiliation", 2);
		weights.put("relationship", 3);
		weights.put("publicationDataset", 4);
		weights.put("similarity", 5);

		weights.put("provision", 6);
		weights.put("participation", 7);
		weights.put("dedup", 8);
	}

	private String groupingKey;

	private String source;

	private String target;

	private String subRelType;

	public String getSource() {
		return source;
	}

	public static SortableRelationKey create(Relation r, String groupingKey) {
		SortableRelationKey sr = new SortableRelationKey();
		sr.setGroupingKey(groupingKey);
		sr.setSource(r.getSource());
		sr.setTarget(r.getTarget());
		sr.setSubRelType(r.getSubRelType());
		return sr;
	}

	@Override
	public int compareTo(SortableRelationKey o) {
		final Integer wt = Optional.ofNullable(weights.get(getSubRelType())).orElse(Integer.MAX_VALUE);
		final Integer wo = Optional.ofNullable(weights.get(o.getSubRelType())).orElse(Integer.MAX_VALUE);
		return ComparisonChain
			.start()
			.compare(wt, wo)
			.compare(getSource(), o.getSource())
			.compare(getTarget(), o.getTarget())
			.result();
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public String getSubRelType() {
		return subRelType;
	}

	public void setSubRelType(String subRelType) {
		this.subRelType = subRelType;
	}

	public String getGroupingKey() {
		return groupingKey;
	}

	public void setGroupingKey(String groupingKey) {
		this.groupingKey = groupingKey;
	}
}
