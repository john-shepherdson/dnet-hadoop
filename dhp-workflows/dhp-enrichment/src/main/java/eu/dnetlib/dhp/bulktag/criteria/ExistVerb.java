
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import org.apache.commons.lang3.StringUtils;

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

	private Boolean isEmpty(Object value){
		if (value instanceof List<?> lista ) {
			return lista == null || lista.stream().allMatch(l -> {
				if (l instanceof String s)
					return StringUtils.isEmpty(s);
				return l == null;
			});
		}
		if (value instanceof String s)
			return StringUtils.isEmpty(s);
		return value == null;
	}

	@Override
	public boolean apply(Object value) {
		//in questo caso esiste un valore indipendentemente dal valore dato
		if(params == null && jsonPath == null) {
			return !isEmpty(value);
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
			Object results;
			try{
				 results = ctx.read(jsonPath);
			}catch(PathNotFoundException e){
				return false;
			}

			if(params == null) {
				return !isEmpty(results);
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
		//si verifica che il valore sia presente in almeno un valore all'interno di otherEntityValue
		//value deve essere una String
		//otherEntityValue puo' essere una string o una lista di stringhe.

		if (value instanceof String stringValue){
			if(otherEntityValue instanceof String stringOtherEntity) {
				return stringValue.trim().equalsIgnoreCase(stringOtherEntity.trim());
			}
			if(otherEntityValue instanceof List<?> lista){
				return lista.stream().anyMatch(e -> (e instanceof String stringOtherEntity) && stringValue.trim().equalsIgnoreCase(stringOtherEntity.trim()));
			}
			else {
				throw new RuntimeException("Parameters not allowed for this verb");
			}
		}
		else {
			throw new RuntimeException("Parameters not allowed for this verb");
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
