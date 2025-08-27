
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.dnetlib.dhp.bulktag.criteria.JsonPathAware;
import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

public class Constraint implements Serializable {
	private String verb;
	private String field;
	private Object value;
	private String jsonpath;
//	private String element;
	@JsonIgnore
	private Selection selection;

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

//@JsonIgnore
	// public void setSelection(Selection sel) {
//		selection = sel;
//	}
	@JsonIgnore
	public void setSelection(VerbResolver resolver)
		throws InvocationTargetException, NoSuchMethodException, InstantiationException,
		IllegalAccessException {
		selection = resolver.getSelectionCriteria(verb, value);

		if (selection instanceof JsonPathAware jpa) {
			jpa.setJsonPath(jsonpath);
		}
	}

	public boolean verifyCriteria(Object metadata) {
		return selection.apply(metadata);
	}

}
