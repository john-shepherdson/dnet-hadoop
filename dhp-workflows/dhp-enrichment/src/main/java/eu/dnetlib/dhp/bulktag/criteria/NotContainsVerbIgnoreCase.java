
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.List;

@VerbClass("not_contains_caseinsensitive")
public class NotContainsVerbIgnoreCase implements Selection, Serializable {

	private List<String> params;

	public NotContainsVerbIgnoreCase() {
	}

	public NotContainsVerbIgnoreCase(final String param) {
		this.params = List.of(param);
	}
	public NotContainsVerbIgnoreCase(final List<String> params) {
		this.params = params;
	}

	public NotContainsVerbIgnoreCase(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}
	@Override
	public boolean apply(Object value) {
		if(value instanceof String s)
			return params.stream().noneMatch(p -> s.toLowerCase().contains(p.toLowerCase())) ;

		if (value instanceof List<?> lista)
			return lista.stream().allMatch(l ->{
				if(l instanceof String s)
					return params.stream().allMatch(p ->
							s.toLowerCase().contains(p.toLowerCase()));
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
