
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

@VerbClass("contains")
public class ContainsVerb implements Selection, Serializable {

	private String param;

	public ContainsVerb() {
	}

	public ContainsVerb(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return value.contains(param);
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
