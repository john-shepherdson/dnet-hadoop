
package eu.dnetlib.dhp.selectioncriteria;

import java.io.Serializable;

@VerbClass("not_equals")
public class NotEqualVerb implements Selection, Serializable {

	private String param;

	public NotEqualVerb(final String param) {
		this.param = param;
	}

	public NotEqualVerb() {
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return !value.equals(param);
	}
}
