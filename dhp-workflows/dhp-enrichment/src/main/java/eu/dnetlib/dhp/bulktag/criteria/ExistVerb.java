
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
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
		if(params == null)
			return jsonPath != null && existAny(jsonPath, value);

		return exist(value, jsonPath);

	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}

	private boolean existAny(String jsonPath, Object value) {

			ReadContext ctx;
			ctx = JsonPath.parse((String)value);

			// estraggo i valori usando il jsonpath
			Object results = ctx.read(jsonPath);

			if (results == null || (results instanceof List<?> lista &&  lista.isEmpty())) {
				return false;
			}
			return true;

	}

	private boolean exist(Object value, String jsonPath) {
		ReadContext ctx;

		if(value instanceof String s)
			ctx = JsonPath.parse(s);
		else
			ctx = JsonPath.parse(value);

		// estraggo i valori usando il jsonpath
		Object results = ctx.read(jsonPath);

		if (results == null || (results instanceof List<?> lista && lista.isEmpty())) {
			return false;
		}

		// confronto i risultati con "value"
		if (params == null) {
			return false;
		} else {
			if (params instanceof String s){
				if(results instanceof List<?> lista)
					return lista.stream().anyMatch(v -> s.equals(v.toString()));
				if(results instanceof String s1)
					return s.equals(s1);
				return false;

			}
			if(params instanceof List<?> paramsList){
				if(results instanceof List<?> lista)
					return lista.stream().anyMatch(paramsList::contains);
				if(results instanceof String s)
					return paramsList.contains(s);
			}
			return false;

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
