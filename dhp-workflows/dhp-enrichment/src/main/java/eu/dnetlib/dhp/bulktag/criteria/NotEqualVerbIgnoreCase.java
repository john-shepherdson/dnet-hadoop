
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("not_equals_caseinsensitive")
public class NotEqualVerbIgnoreCase implements Selection, Serializable {

	private String param;

	public NotEqualVerbIgnoreCase(final String param) {
		this.param = param;
	}

	public NotEqualVerbIgnoreCase() {
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return !value.equalsIgnoreCase(param);
	}
}
