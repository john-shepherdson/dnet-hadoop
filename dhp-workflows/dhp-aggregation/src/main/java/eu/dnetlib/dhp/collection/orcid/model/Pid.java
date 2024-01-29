
package eu.dnetlib.dhp.collection.orcid.model;

public class Pid {

	private String value;

	private String schema;

	public Pid() {
	}

	public Pid(String value, String schema) {
		this.value = value;
		this.schema = schema;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}
}
