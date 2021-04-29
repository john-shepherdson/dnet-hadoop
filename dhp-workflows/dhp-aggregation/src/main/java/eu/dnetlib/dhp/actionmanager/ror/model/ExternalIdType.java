
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalIdType implements Serializable {

	@JsonProperty("all")
	private List<String> all = new ArrayList<>();

	@JsonProperty("preferred")
	private String preferred;

	private final static long serialVersionUID = 2616688352998387611L;

	public List<String> getAll() {
		return all;
	}

	public void setAll(final List<String> all) {
		this.all = all;
	}

	public String getPreferred() {
		return preferred;
	}

	public void setPreferred(final String preferred) {
		this.preferred = preferred;
	}

}
