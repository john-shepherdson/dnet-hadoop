
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_equals")
public class NotEqualVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public NotEqualVerb(final String param) {
		this.params = List.of(param);
	}
	public NotEqualVerb(final List<String> param) {
		this.params = param;
	}

	public NotEqualVerb() {
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(params.size() == 1){
			if (value instanceof String s) {
				return ! params.get(0).equals(s);
			}
			if(value instanceof List<?> list )
				if(list.size() == 1 && list.get(0) instanceof String s )
					return ! params.get(0).equals(s);
		}

		return true;
	}
}
