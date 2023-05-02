
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("equals_caseinsensitive")
public class EqualVerbIgnoreCase implements Selection, Serializable {

	private String param;

	public EqualVerbIgnoreCase() {
	}

	public EqualVerbIgnoreCase(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return value.equalsIgnoreCase(param);
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
