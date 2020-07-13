
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class SortableRelationKey implements Comparable<SortableRelationKey>, Serializable {

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put("participation", 0);

		weights.put("outcome", 1);
		weights.put("affiliation", 2);
		weights.put("dedup", 3);
		weights.put("publicationDataset", 4);
		weights.put("citation", 5);
		weights.put("supplement", 6);
		weights.put("review", 7);
		weights.put("relationship", 8);
		weights.put("provision", 9);
		weights.put("similarity", 10);
	}

	private static final long serialVersionUID = 3232323;

	private String groupingKey;

	private String subRelType;

	public static SortableRelationKey create(Relation r, String groupingKey) {
		SortableRelationKey sr = new SortableRelationKey();
		sr.setGroupingKey(groupingKey);
		sr.setSubRelType(r.getSubRelType());
		return sr;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		SortableRelationKey that = (SortableRelationKey) o;
		return getGroupingKey().equals(that.getGroupingKey());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getGroupingKey());
	}

	@Override
	public int compareTo(SortableRelationKey o) {
		return ComparisonChain
			.start()
			.compare(getGroupingKey(), o.getGroupingKey())
			.compare(getWeight(this), getWeight(o))
			.result();
	}

	private Integer getWeight(SortableRelationKey o) {
		return Optional.ofNullable(weights.get(o.getSubRelType())).orElse(Integer.MAX_VALUE);
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
