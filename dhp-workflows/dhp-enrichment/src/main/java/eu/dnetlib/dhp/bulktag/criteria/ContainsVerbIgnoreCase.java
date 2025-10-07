
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("contains_caseinsensitive")
public class ContainsVerbIgnoreCase implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public ContainsVerbIgnoreCase() {
	}

	public ContainsVerbIgnoreCase(final String param) {
		this.params = List.of(param);
	}
	public ContainsVerbIgnoreCase(final List<String> params) {
		this.params = params;
	}

	public ContainsVerbIgnoreCase(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}

	@Override
	public boolean apply(Object value) {
		if(value instanceof String s)
			return isAllMatch(s);

		//todo in questo caso bisogna capire se abbiamo un POJO o una lista di stringhe
		//comunque basta che almeno un elemento nella lista contenga tutti i valori
		//passati nel constraint
		if(value instanceof List<?> lista)
			return lista.stream().anyMatch(l -> {
				if(l instanceof String s)
					return isAllMatch(s);
				return false;
			});


		return false;
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		return false;
	}

	private boolean isAllMatch(String s) {
		return params.stream().allMatch(p -> s.toLowerCase().contains(p.toLowerCase()));
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
