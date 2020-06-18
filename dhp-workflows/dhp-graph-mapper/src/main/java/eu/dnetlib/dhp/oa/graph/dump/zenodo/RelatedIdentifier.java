
package eu.dnetlib.dhp.oa.graph.dump.zenodo;

import java.io.Serializable;

public class RelatedIdentifier implements Serializable {
	private String identifier;
	private String relation;
	private String resource_type;
	private String scheme;

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	public String getResource_type() {
		return resource_type;
	}

	public void setResource_type(String resource_type) {
		this.resource_type = resource_type;
	}

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}
}
