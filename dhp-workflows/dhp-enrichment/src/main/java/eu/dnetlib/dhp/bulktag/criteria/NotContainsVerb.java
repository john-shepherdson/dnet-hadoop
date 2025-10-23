
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_contains")
public class NotContainsVerb implements Selection, Serializable {

	private Object params = new ArrayList<>();

	public NotContainsVerb() {
	}

	public NotContainsVerb(final Object param) {
		this.params = param;
	}
	@Override
	public boolean apply(Object value) {
		ContainsVerb contains = new ContainsVerb(params);
		return !contains.apply(value);
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}

	public Object getParam() {
		return params;
	}

	public void setParam(Object param) {
		this.params = param;
	}
}
