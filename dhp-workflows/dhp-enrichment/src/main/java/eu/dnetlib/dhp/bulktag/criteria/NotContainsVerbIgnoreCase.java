
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("not_contains_caseinsensitive")
public class NotContainsVerbIgnoreCase implements Selection, Serializable {

	private String param;

	public NotContainsVerbIgnoreCase() {
	}

	public NotContainsVerbIgnoreCase(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return !(value.toLowerCase().contains(param.toLowerCase()));
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
