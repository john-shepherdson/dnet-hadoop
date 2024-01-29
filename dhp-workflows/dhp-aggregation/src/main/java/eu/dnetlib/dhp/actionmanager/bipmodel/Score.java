
package eu.dnetlib.dhp.actionmanager.bipmodel;

import java.io.Serializable;
import java.util.List;

/**
 * represents the score in the input file
 */
public class Score implements Serializable {

	private String id;
	private List<KeyValue> unit;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<KeyValue> getUnit() {
		return unit;
	}

	public void setUnit(List<KeyValue> unit) {
		this.unit = unit;
	}
}
