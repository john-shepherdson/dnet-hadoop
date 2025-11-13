
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.dnetlib.dhp.bulktag.criteria.ApplyOtherVerbAware;
import eu.dnetlib.dhp.bulktag.criteria.JsonPathAware;
import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

public class Constraint implements Serializable {
	private String verb;
	private String field;
	private Object value;
	private String jsonPath;
	private String applyVerb;

	public String getApplyVerb() {
		return applyVerb;
	}

	public void setApplyVerb(String applyVerb) {
		this.applyVerb = applyVerb;
	}

	@JsonIgnore
	private Selection selection;

	public String getJsonPath() {
		return jsonPath;
	}

	public void setJsonPath(String jsonPath) {
		this.jsonPath = jsonPath;
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
			jpa.setJsonPath(jsonPath);
		}
		if (selection instanceof ApplyOtherVerbAware aova)
			aova.setApplyVerb(applyVerb);
	}

	public boolean verifyCriteria(Object metadata) {
		try {
			return selection.apply(metadata);
		}catch (Exception e){
			return false;
		}
	}

	public boolean verifyCriteria(Object metadata, Object value) {
		try {
			return selection.apply(metadata, value);
		}catch (Exception e){
			return false;
		}
	}

	public void setVerbJsonPath(String jsonPath){
		if( selection instanceof JsonPathAware jpa)
			jpa.setJsonPath(jsonPath);

	}

}
