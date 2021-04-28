
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class GridType implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -5605887658267581353L;

	private String all;
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
