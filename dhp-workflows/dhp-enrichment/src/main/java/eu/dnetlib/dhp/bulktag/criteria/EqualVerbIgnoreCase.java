
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("equals_caseinsensitive")
public class EqualVerbIgnoreCase implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public EqualVerbIgnoreCase() {
	}

	public EqualVerbIgnoreCase(final String param) {
		this.params = List.of(param);
	}
	public EqualVerbIgnoreCase(final List<String> param) {
		this.params = param;
	}


	@Override
	public boolean apply(Object value) {
		// Only if value is an instance of String the comparison can be done
		if(params.size() == 1){
			if (value instanceof String s) {
				return params.get(0).equalsIgnoreCase(s);
			}
			if(value instanceof List<?> list )
				if(list.size() == 1 && list.get(0) instanceof String s )
					return params.get(0).equalsIgnoreCase(s);
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
