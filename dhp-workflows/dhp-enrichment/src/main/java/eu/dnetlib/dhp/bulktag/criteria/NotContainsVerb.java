
package eu.dnetlib.dhp.bulktag.criteria;

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

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
