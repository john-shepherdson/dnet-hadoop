
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
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

	public StartsWithVerb(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
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
