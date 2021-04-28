
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class Label implements Serializable {

	private String iso639;
	private String label;
	private final static long serialVersionUID = -6576156103297850809L;

	public String getIso639() {
		return iso639;
	}

	public void setIso639(final String iso639) {
		this.iso639 = iso639;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(final String label) {
		this.label = label;
	}

}
