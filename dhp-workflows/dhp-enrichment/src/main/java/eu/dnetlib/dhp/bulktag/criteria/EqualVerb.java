
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@VerbClass("equals")
public class EqualVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public EqualVerb() {
	}

	public EqualVerb(final String param) {
		this.params = List.of(param);
	}
	public EqualVerb(final List<String> param) {
		this.params = param;
	}

	public EqualVerb(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}
	@Override
	public boolean apply(Object value) {
		// Only if value is an instance of String the comparison can be done
		if(params.size() == 1){
			if (value instanceof String s) {
				return params.get(0).equals(s);
			}
			if(value instanceof List<?> list )
				if(list.size() == 1 && list.get(0) instanceof String s )
					return params.get(0).equals(s);
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
