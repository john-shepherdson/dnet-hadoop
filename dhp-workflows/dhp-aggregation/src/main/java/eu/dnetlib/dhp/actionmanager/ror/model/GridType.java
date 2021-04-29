
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GridType implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -5605887658267581353L;

	@JsonProperty("all")
	private String all;

	@JsonProperty("preferred")
	private String preferred;

	public String getAll() {
		return all;
	}

	public void setAll(final String all) {
		this.all = all;
	}

	public String getPreferred() {
		return preferred;
	}

	public void setPreferred(final String preferred) {
		this.preferred = preferred;
	}

}
