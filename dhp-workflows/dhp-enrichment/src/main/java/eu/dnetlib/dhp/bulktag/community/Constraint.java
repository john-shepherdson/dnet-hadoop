
package eu.dnetlib.dhp.bulktag.community;

import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class Constraint implements Serializable {
	private String verb;
	private String field;
	private String value;
	private Selection selection;

	public Constraint() {
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setSelection(Selection sel) {
		selection = sel;
	}

	public void setSelection(VerbResolver resolver)
		throws InvocationTargetException, NoSuchMethodException, InstantiationException,
		IllegalAccessException {
		selection = resolver.getSelectionCriteria(verb, value);
	}

	public boolean verifyCriteria(String metadata) {
		return selection.apply(metadata);
	}
}
