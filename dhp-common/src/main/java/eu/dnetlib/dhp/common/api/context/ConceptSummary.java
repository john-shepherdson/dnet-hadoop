
package eu.dnetlib.dhp.common.api.context;

import java.util.List;

public class ConceptSummary {

	private String id;

	private String label;

	public boolean hasSubConcept;

	private List<ConceptSummary> concepts;

	public String getId() {
		return id;
	}

	public String getLabel() {
		return label;
	}

	public List<ConceptSummary> getConcepts() {
		return concepts;
	}

	public ConceptSummary setId(final String id) {
		this.id = id;
		return this;
	}

	public ConceptSummary setLabel(final String label) {
		this.label = label;
		return this;
	}

	public boolean isHasSubConcept() {
		return hasSubConcept;
	}

	public ConceptSummary setHasSubConcept(final boolean hasSubConcept) {
		this.hasSubConcept = hasSubConcept;
		return this;
	}

	public ConceptSummary setConcept(final List<ConceptSummary> concepts) {
		this.concepts = concepts;
		return this;
	}

}
