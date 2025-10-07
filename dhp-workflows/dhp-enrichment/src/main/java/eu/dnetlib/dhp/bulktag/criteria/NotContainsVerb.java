
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_contains")
public class NotContainsVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public NotContainsVerb() {
	}

	public NotContainsVerb(final String param) {
		this.params = List.of(param);
	}
	public NotContainsVerb(final List<String> params) {
		this.params = params;
	}

	public NotContainsVerb(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}
	@Override
	public boolean apply(Object value) {
		if(value instanceof String s)
			return params.stream().noneMatch(s::contains);

		if(value instanceof List<?> lista)
			return lista.stream().allMatch(l -> {
				if(l instanceof String s)
					return params.stream().noneMatch(s::contains);
				return false;
			});
		return false;
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
