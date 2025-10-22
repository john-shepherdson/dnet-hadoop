
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;

//Verifica che almeno un valore in una lista sia uguale al valore dato oppure che esista un valore indipendentemente da un valore dato
//per esempio esiste una data
@VerbClass("not_exist")
public class NotExistVerb implements Selection, JsonPathAware, Serializable {
	private String jsonPath;
	private Object params ;

	public NotExistVerb() {
	}

	public NotExistVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		return value == null;

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

	@Override
	public void setJsonPath(String jsonpath) {
		this.jsonPath = jsonpath;
	}
}
