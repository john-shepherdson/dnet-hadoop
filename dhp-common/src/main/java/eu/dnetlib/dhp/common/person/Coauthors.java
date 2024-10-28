
package eu.dnetlib.dhp.common.person;

import java.io.Serializable;
import java.util.List;

public class Coauthors implements Serializable {
	private List<String> coauthors;

	public List<String> getCoauthors() {
		return coauthors;
	}

	public void setCoauthors(List<String> coauthors) {
		this.coauthors = coauthors;
	}
}
