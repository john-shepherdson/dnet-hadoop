
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.List;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

//Verifica che almeno un valore in una lista sia uguale al valore dato oppure che esista un valore indipendentemente da un valore dato
//per esempio esiste una data
@VerbClass("exist")
public class ExistVerb implements Selection, JsonPathAware, Serializable {
	private String jsonPath;
	private Object params ;

	public ExistVerb() {
	}

	public ExistVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(value == null)
			return jsonPath != null && existAny(jsonPath);

		return exist(value, jsonPath);

	}

	private boolean existAny(String jsonPath) {

			ReadContext ctx;
			ctx = JsonPath.parse(params);
			// estraggo i valori usando il jsonpath
			List<Object> results = ctx.read(jsonPath);

			if (results == null || results.isEmpty()) {
				return false;
			}
			return true;

	}

	private boolean exist(Object value, String jsonPath) {
		ReadContext ctx;

		ctx = JsonPath.parse(params);

		// estraggo i valori usando il jsonpath
		List<Object> results = ctx.read(jsonPath);

		if (results == null || results.isEmpty()) {
			return false;
		}

		// confronto i risultati con "value"
		if (value == null) {
			return false;
		} else {
			return results.stream().anyMatch(v -> value.equals(v.toString()));
		}
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
