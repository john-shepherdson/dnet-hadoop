
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OpenDoarRepoStatus implements Serializable {

	private static final long serialVersionUID = 4832658700366871160L;

	private String id;

	private String jurisdiction;

	private boolean fromBase = false;

	private boolean highCompliance = false;

	private boolean baseMAX = false;

	private Map<String, Long> aggregations = new HashMap<>();

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getJurisdiction() {
		return this.jurisdiction;
	}

	public void setJurisdiction(final String jurisdiction) {
		this.jurisdiction = jurisdiction;
	}

	public Map<String, Long> getAggregations() {
		return this.aggregations;
	}

	public void setAggregations(final Map<String, Long> aggregations) {
		this.aggregations = aggregations;
	}

	public boolean isHighCompliance() {
		return this.highCompliance;
	}

	public void setHighCompliance(final boolean highCompliance) {
		this.highCompliance = highCompliance;
	}

	public boolean isFromBase() {
		return this.fromBase;
	}

	public void setFromBase(final boolean fromBase) {
		this.fromBase = fromBase;
	}

	public boolean isBaseMAX() {
		return this.baseMAX;
	}

	public void setBaseMAX(final boolean baseMAX) {
		this.baseMAX = baseMAX;
	}
}
