
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("starts_with")
public class StartsWithVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public StartsWithVerb() {
	}

	public StartsWithVerb(final String param) {
		this.params = List.of(param);
	}
	public StartsWithVerb(final List<String> param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(params.size() == 1){
			if(value instanceof String s)
				return s.startsWith(params.get(0));
			if(value instanceof List<?> lista && lista.size() == 1 && lista.get(0) instanceof String s)
				return s.startsWith(params.get(0));
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
