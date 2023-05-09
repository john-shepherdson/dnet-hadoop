
package eu.dnetlib.dhp.oa.provision;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SortableRelation extends Relation implements Comparable<SortableRelation>, Serializable {

	private static final Map<Relation.SUBRELTYPE, Integer> weights = Maps.newHashMap();

	//TODO version and part missing why publication is there?

	static {
		weights.put(Relation.SUBRELTYPE.outcome, 0);
		weights.put(Relation.SUBRELTYPE.supplement, 1);
		weights.put(Relation.SUBRELTYPE.review, 2);
		weights.put(Relation.SUBRELTYPE.citation, 3);
		weights.put(Relation.SUBRELTYPE.affiliation, 4);
		weights.put(Relation.SUBRELTYPE.relationship, 5);
		//weights.put(Result.RESULTTYPE.publication, 6);
		weights.put(Relation.SUBRELTYPE.similarity, 7);

		weights.put(Relation.SUBRELTYPE.provision, 8);
		weights.put(Relation.SUBRELTYPE.participation, 9);
		weights.put(Relation.SUBRELTYPE.dedup, 10);
	}
	private static final long serialVersionUID = 34753984579L;

	private String groupingKey;

	public static SortableRelation create(Relation r, String groupingKey) {
		SortableRelation sr = new SortableRelation();
		sr.setGroupingKey(groupingKey);
		sr.setSource(r.getSource());
		sr.setTarget(r.getTarget());
		sr.setRelType(r.getRelType());
		sr.setSubRelType(r.getSubRelType());
		sr.setRelClass(r.getRelClass());
		sr.setProvenance(r.getProvenance());
		sr.setProperties(r.getProperties());
		sr.setValidated(r.getValidated());
		sr.setValidationDate(r.getValidationDate());

		return sr;
	}

	@JsonIgnore
	public Relation asRelation() {
		return this;
	}

	@Override
	public int compareTo(SortableRelation o) {
		return ComparisonChain
			.start()
			.compare(getGroupingKey(), o.getGroupingKey())
			.compare(getWeight(this), getWeight(o))
			.result();
	}

	private Integer getWeight(SortableRelation o) {
		return Optional.ofNullable(weights.get(o.getSubRelType())).orElse(Integer.MAX_VALUE);
	}

	public String getGroupingKey() {
		return groupingKey;
	}

	public void setGroupingKey(String groupingKey) {
		this.groupingKey = groupingKey;
	}
}
