
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("starts_with")
public class StartsWithVerb implements Selection, Serializable {

	private String param;

	public StartsWithVerb() {
	}

	public StartsWithVerb(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return value.startsWith(param);
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
