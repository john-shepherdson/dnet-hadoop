
package eu.dnetlib.dhp.actionmanager.personentity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Coauthors implements Serializable {
	private List<String> coauthors;

	public List<String> getCoauthors() {
		return coauthors;
	}

	public void setCoauthors(List<String> coauthors) {
		this.coauthors = coauthors;
	}
}
