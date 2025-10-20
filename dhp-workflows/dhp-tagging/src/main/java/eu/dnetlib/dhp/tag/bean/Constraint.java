
package eu.dnetlib.dhp.tag.bean;


import java.io.Serializable;

public class Constraint implements Serializable {
	private String verb;
	private String field;
	private Object value;
	private String jsonpath;
	private String where;

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getJsonpath() {
		return jsonpath;
	}

	public void setJsonpath(String jsonpath) {
		this.jsonpath = jsonpath;
	}

	public String getVerb() {
		return verb;
	}

	public void setVerb(String verb) {
		this.verb = verb;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}



}
