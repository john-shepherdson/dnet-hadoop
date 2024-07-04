
package eu.dnetlib.dhp.actionmanager.personentity;

import java.io.Serializable;
import java.util.ArrayList;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Coauthors implements Serializable {
	private ArrayList<Relation> coauthors;

	public ArrayList<Relation> getCoauthors() {
		return coauthors;
	}

	public void setCoauthors(ArrayList<Relation> coauthors) {
		this.coauthors = coauthors;
	}
}
