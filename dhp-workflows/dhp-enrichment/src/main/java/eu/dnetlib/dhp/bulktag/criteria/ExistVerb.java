
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

//Verifica che esista un valore indipendentemente dal valore dato (param = null)
//Verifica che esista fra i valori a disposizione almeno uno uguale ad una lista di valori dati (param = lista).
//i valori possono essere una lista o un valore
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
		//in questo caso esiste un valore indipendentemente dal valore dato
		if(params == null && jsonPath == null) {
			if (value instanceof List<?> lista && lista.isEmpty())
				return false;
			return value != null;
		}
		//il valore esiste in una lista di valori dati.
		//jsonPath e' nullp => value e' una stringa da controllare e params e' una lista di valori
		if(jsonPath == null ){
			if( value instanceof String stringa && params instanceof List<?> lista) {
				boolean ret = lista.stream().anyMatch(entry -> stringa.equalsIgnoreCase((String) entry));
				return ret;
			}
				
			if(value instanceof List<?> valueList && params instanceof List<?> paramsList)
				return !valueList.isEmpty() && valueList.stream().allMatch(entryValue -> paramsList.stream().anyMatch(entryParams -> entryParams.equals(entryValue)));
			else
				throw new RuntimeException("Not possible to apply exist on this parameters");
		}
		//jsonPath non e' nullo -> value puo' essere una lista da ottenere con il jsonPath e l'esistenza va bene se almeno uno dei valori nella lista e' dentro params
		else {
			ReadContext ctx;
			if(value instanceof String)
				ctx = JsonPath.parse((String)value);
			else
				ctx = JsonPath.parse(value);
			Object results = ctx.read(jsonPath);
			if (results == null || (results instanceof List<?> lista &&  lista.isEmpty())) {
				return false;
			}
			if(params == null) {
				return true;
			}
			if (results instanceof String stringa && params instanceof List<?> lista) {
				return lista.stream().anyMatch(entry -> stringa.equalsIgnoreCase((String) entry));
			}
			else {
				if(results instanceof List<?> lista && params instanceof String stringa ) {
					return !lista.isEmpty() && lista.stream().anyMatch(entry -> stringa.equalsIgnoreCase((String)entry));
				}

				else {
					if(results instanceof List<?> resultList && params instanceof List<?> paramsList) {
						return !resultList.isEmpty() && resultList.stream().allMatch(resultEntry -> paramsList.stream().anyMatch(resultEntry::equals));
					}
				}
			}

		}

		throw new RuntimeException("Not possible to apply exist on these parameters");
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
