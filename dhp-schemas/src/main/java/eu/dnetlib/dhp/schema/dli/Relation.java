package eu.dnetlib.dhp.schema.dli;

import java.io.Serializable;
import java.util.List;

public class Relation implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -9103706796710618813L;

	private String source;

	private String target;

	private List<Provenance> provenance;

	private RelationSemantic semantic;

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(final String target) {
		this.target = target;
	}

	public List<Provenance> getProvenance() {
		return provenance;
	}

	public void setProvenance(final List<Provenance> provenance) {
		this.provenance = provenance;
	}

	public RelationSemantic getSemantic() {
		return semantic;
	}

	public void setSemantic(final RelationSemantic semantic) {
		this.semantic = semantic;
	}
}
