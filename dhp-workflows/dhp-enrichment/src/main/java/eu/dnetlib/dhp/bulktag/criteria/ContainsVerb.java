
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
//Nuova implementazione. Sia il valore nei constraints che il field ritornato possono essere
//liste
@VerbClass("contains")
public class ContainsVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public ContainsVerb() {
	}

	public ContainsVerb(final String param) {
		this.params = List.of(param);
	}
	public ContainsVerb(final List<String> params) {
		this.params = params;
	}

	@Override
	public boolean apply(Object value) {
		if(value instanceof String s)
			return params.stream().allMatch(s::contains);

		//todo in questo caso bisogna capire se abbiamo un POJO o una lista di stringhe
		//comunque basta che almeno un elemento nella lista contenga tutti i valori
		//passati nel constraint
		if(value instanceof List<?> lista)
			return lista.stream().anyMatch(l -> {
				if(l instanceof String s)
					return params.stream().allMatch(s::contains);
				return false;
			});


		return false;
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
