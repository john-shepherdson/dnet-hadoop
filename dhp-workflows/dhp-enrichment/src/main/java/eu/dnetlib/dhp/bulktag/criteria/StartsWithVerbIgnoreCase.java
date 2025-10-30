
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("starts_with_caseinsensitive")
public class StartsWithVerbIgnoreCase implements Selection, Serializable {

	private Object params = new ArrayList<>();

	public StartsWithVerbIgnoreCase() {
	}

	public StartsWithVerbIgnoreCase(final Object param) {
		this.params = param;

	}


	@Override
	public boolean apply(Object value) {
		if(params instanceof String sParam){
			if(value instanceof String s  )
				return s.toLowerCase().startsWith(sParam.toLowerCase());
			if (value instanceof List<?> lista )
				return lista.stream().anyMatch(l -> (l instanceof String sValue && sValue.toLowerCase().startsWith(sParam.toLowerCase())));
		}

		return false;
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}

	public Object getParam() {
		return params;
	}

	public void setParam(Object param) {
		this.params = param;
	}
}
