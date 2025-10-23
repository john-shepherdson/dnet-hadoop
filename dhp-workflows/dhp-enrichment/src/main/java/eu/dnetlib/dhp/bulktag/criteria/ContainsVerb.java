
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
//Nuova implementazione. Sia il valore nei constraints che il field ritornato possono essere
//liste
@VerbClass("contains")
public class ContainsVerb implements Selection, Serializable {

	private Object params ;

	public ContainsVerb() {
	}


	public ContainsVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(value instanceof String valueString && params instanceof String paramString)
			return valueString.contains(paramString);
		if(value instanceof String s && params instanceof List<?> lista)
			return lista.stream().allMatch(entry -> s.contains((String)entry));

		//todo in questo caso bisogna capire se abbiamo un POJO o una lista di stringhe
		//comunque basta che almeno un elemento nella lista contenga tutti i valori
		//passati nel constraint
		if(value instanceof List<?> lista && params instanceof String paramString)
			return lista.stream().anyMatch(l -> {
				if(l instanceof String s)
					return s.contains(paramString);
					//return params.stream().allMatch(s::contains);
				return false;
			});

		if(value instanceof List<?> lista && params instanceof List<?> paramsList)
			return lista.stream().allMatch(l -> {
				if(l instanceof String s)
					//return s.contains(paramString);
					return paramsList.stream().anyMatch(entry -> s.contains((String)entry));
				return false;
			});

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
