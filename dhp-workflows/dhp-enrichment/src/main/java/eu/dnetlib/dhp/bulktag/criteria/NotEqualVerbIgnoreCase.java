
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_equals_caseinsensitive")
public class NotEqualVerbIgnoreCase implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public NotEqualVerbIgnoreCase(final String param) {
		this.params = List.of(param);
	}
	public NotEqualVerbIgnoreCase(final List<String> param) {
		this.params = param;
	}

	public NotEqualVerbIgnoreCase(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}
	public NotEqualVerbIgnoreCase() {
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value)
	{
		if(params.size() == 1){
			if(value instanceof String s)
				return !s.equalsIgnoreCase(params.get(0));
			if(value instanceof List<?> list )
				if(list.size() == 1 && list.get(0) instanceof String s )
					return ! params.get(0).equalsIgnoreCase(s);
		}


		return true;
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}
}
