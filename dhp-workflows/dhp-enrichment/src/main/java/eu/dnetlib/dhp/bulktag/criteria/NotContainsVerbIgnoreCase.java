
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
