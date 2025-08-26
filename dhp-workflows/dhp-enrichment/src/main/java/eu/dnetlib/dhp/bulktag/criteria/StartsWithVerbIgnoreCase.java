
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("starts_with_caseinsensitive")
public class StartsWithVerbIgnoreCase implements Selection, Serializable {

	private String param;

	public StartsWithVerbIgnoreCase() {
	}

	public StartsWithVerbIgnoreCase(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(Object value) {
		if(value instanceof String s)
			return s.toLowerCase().startsWith(param.toLowerCase());
		return false;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
