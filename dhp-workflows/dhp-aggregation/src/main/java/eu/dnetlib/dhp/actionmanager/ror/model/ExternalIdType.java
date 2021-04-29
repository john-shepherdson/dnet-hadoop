
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalIdType implements Serializable {

	@JsonProperty("all")
	private Object all;

	@JsonProperty("preferred")
	private String preferred;

	private final static long serialVersionUID = 2616688352998387611L;

	public Object getAll() {
		return all;
	}

	public void setAll(final Object all) {
		this.all = all;
	}

	public String getPreferred() {
		return preferred;
	}

	public void setPreferred(final String preferred) {
		this.preferred = preferred;
	}

}
