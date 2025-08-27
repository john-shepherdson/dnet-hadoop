
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("starts_with_caseinsensitive")
public class StartsWithVerbIgnoreCase implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public StartsWithVerbIgnoreCase() {
	}

	public StartsWithVerbIgnoreCase(final String param) {
		this.params = List.of(param);
	}
	public StartsWithVerbIgnoreCase(final List<String> param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(params.size() == 1){
			if(value instanceof String s)
				return s.toLowerCase().startsWith(params.get(0).toLowerCase());
			if (value instanceof List<?> lista && lista.size() == 1 && lista.get(0) instanceof String s)
				return s.toLowerCase().startsWith(params.get(0).toLowerCase());
		}

		return false;
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
