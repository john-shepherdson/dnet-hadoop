
package eu.dnetlib.dhp.schema.scholexplorer;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class DLIRelation extends Relation {
	private String dateOfCollection;

	public String getDateOfCollection() {
		return dateOfCollection;
	}

	public void setDateOfCollection(String dateOfCollection) {
		this.dateOfCollection = dateOfCollection;
	}
}
