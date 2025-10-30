
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_equals_caseinsensitive")
public class NotEqualVerbIgnoreCase implements Selection, Serializable {

	private Object params ;


	public NotEqualVerbIgnoreCase(final Object param) {
		this.params = param;
	}
	public NotEqualVerbIgnoreCase() {
	}

	public Object getParam() {
		return params;
	}

	public void setParam(Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value)
	{
		if(params instanceof String sParam){
			if(value instanceof String s  )
				return !s.trim().equalsIgnoreCase(sParam.trim());
			if (value instanceof List<?> lista )
				return lista.stream().allMatch(l -> (l instanceof String sValue && !sValue.trim().equalsIgnoreCase(sParam.trim())));
		}


		throw new RuntimeException("Verb not applicable with this configuration");
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}
}
