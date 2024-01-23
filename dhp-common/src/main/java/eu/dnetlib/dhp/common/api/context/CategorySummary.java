
package eu.dnetlib.dhp.common.api.context;

public class CategorySummary {

	private String id;

	private String label;

	private boolean hasConcept;

	public String getId() {
		return id;
	}

	public String getLabel() {
		return label;
	}

	public boolean isHasConcept() {
		return hasConcept;
	}

	public CategorySummary setId(final String id) {
		this.id = id;
		return this;
	}

	public CategorySummary setLabel(final String label) {
		this.label = label;
		return this;
	}

	public CategorySummary setHasConcept(final boolean hasConcept) {
		this.hasConcept = hasConcept;
		return this;
	}

}
