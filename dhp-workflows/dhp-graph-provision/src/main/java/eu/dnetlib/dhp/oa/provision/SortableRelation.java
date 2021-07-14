
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

	private static final Map<String, Integer> weights = Maps.newHashMap();

	static {
		weights.put(ModelConstants.OUTCOME, 0);
		weights.put(ModelConstants.SUPPLEMENT, 1);
		weights.put(ModelConstants.REVIEW, 2);
		weights.put(ModelConstants.CITATION, 3);
		weights.put(ModelConstants.AFFILIATION, 4);
		weights.put(ModelConstants.RELATIONSHIP, 5);
		weights.put(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID, 6);
		weights.put(ModelConstants.SIMILARITY, 7);

		weights.put(ModelConstants.PROVISION, 8);
		weights.put(ModelConstants.PARTICIPATION, 9);
		weights.put(ModelConstants.DEDUP, 10);
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
		sr.setDataInfo(r.getDataInfo());
		sr.setCollectedfrom(r.getCollectedfrom());
		sr.setLastupdatetimestamp(r.getLastupdatetimestamp());
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
