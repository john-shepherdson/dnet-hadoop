
package eu.dnetlib.dhp.bulktag.criteria;

import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

//Verifica tutti i componenti di una lista abbiano nel valore espresso dal jsonpath almeno uno dei valori espressi nel param
@VerbClass("exist_forall")
public class ExistForAllVerb implements Selection, JsonPathAware, Serializable {
	private String jsonPath;
	private Object params ;

	public ExistForAllVerb() {
	}

	public ExistForAllVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value)  {

        List<Object> parsed = null;
        try {
            parsed = new ObjectMapper().readValue((String)value, List.class);
        } catch (IOException e) {
            return false;
        }

        return parsed.stream().allMatch(o -> {
			ExistVerb exist = new ExistVerb(params);
			exist.setJsonPath(jsonPath);
			return exist.apply(o);
		});


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
