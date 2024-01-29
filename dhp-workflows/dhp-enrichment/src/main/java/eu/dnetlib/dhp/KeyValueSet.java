
package eu.dnetlib.dhp;

import java.io.Serializable;
import java.util.ArrayList;

public class KeyValueSet implements Serializable {
	private String key;
	private ArrayList<String> valueSet;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public ArrayList<String> getValueSet() {
		return valueSet;
	}

	public void setValueSet(ArrayList<String> valueSet) {
		this.valueSet = valueSet;
	}
}
