
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@VerbClass("equals")
public class EqualVerb implements Selection, Serializable {

	private String param;

	public EqualVerb() {
	}

	public EqualVerb(final String param) {
		this.param = param;
	}


	@Override
	public boolean apply(Object value) {
		// Only if value is an instance of String the comparison can be done
		if (value instanceof String s ) {
			return param.equals(s);
		}

		return false;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
