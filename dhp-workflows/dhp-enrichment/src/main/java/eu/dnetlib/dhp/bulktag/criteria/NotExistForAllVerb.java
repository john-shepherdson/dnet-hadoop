
package eu.dnetlib.dhp.bulktag.criteria;

import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

//Verifica tutti i componenti di una lista abbiano nel valore espresso dal jsonpath almeno uno dei valori espressi nel param
@VerbClass("not_exist_forall")
public class NotExistForAllVerb implements Selection, JsonPathAware, Serializable {
	private String jsonPath;
	private Object params ;

	public NotExistForAllVerb() {
	}

	public NotExistForAllVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value)  {
		ExistForAllVerb efa = new ExistForAllVerb(params);
		efa.setJsonPath(jsonPath);
        return !efa.apply(value);


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
