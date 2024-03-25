
package eu.dnetlib.dhp.common.api.context;

public class ContextSummary {

	private String id;

	private String label;

	private String type;

	private String status;

	public String getId() {
		return id;
	}

	public String getLabel() {
		return label;
	}

	public String getType() {
		return type;
	}

	public String getStatus() {
		return status;
	}

	public ContextSummary setId(final String id) {
		this.id = id;
		return this;
	}

	public ContextSummary setLabel(final String label) {
		this.label = label;
		return this;
	}

	public ContextSummary setType(final String type) {
		this.type = type;
		return this;
	}

	public ContextSummary setStatus(final String status) {
		this.status = status;
		return this;
	}

}
