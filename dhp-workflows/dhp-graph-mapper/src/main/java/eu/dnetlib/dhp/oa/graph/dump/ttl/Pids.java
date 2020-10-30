
package eu.dnetlib.dhp.oa.graph.dump.ttl;

import java.io.Serializable;

public class Pids implements Serializable {
	private String type;
	private String value;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
