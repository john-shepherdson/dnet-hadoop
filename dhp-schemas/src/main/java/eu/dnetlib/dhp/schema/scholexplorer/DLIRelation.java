
package eu.dnetlib.dhp.schema.scholexplorer;

import java.util.List;

import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class DLIRelation extends Relation {

	private String dateOfCollection;

	private List<KeyValue> collectedFrom;

	public List<KeyValue> getCollectedFrom() {
		return collectedFrom;
	}

	public void setCollectedFrom(List<KeyValue> collectedFrom) {
		this.collectedFrom = collectedFrom;
	}

	public String getDateOfCollection() {
		return dateOfCollection;
	}

	public void setDateOfCollection(String dateOfCollection) {
		this.dateOfCollection = dateOfCollection;
	}
}
