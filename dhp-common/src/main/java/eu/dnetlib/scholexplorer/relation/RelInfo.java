
package eu.dnetlib.scholexplorer.relation;

import java.io.Serializable;

public class RelInfo implements Serializable {
	private String original;
	private String inverse;

	public String getOriginal() {
		return original;
	}

	public void setOriginal(String original) {
		this.original = original;
	}

	public String getInverse() {
		return inverse;
	}

	public void setInverse(String inverse) {
		this.inverse = inverse;
	}
}
