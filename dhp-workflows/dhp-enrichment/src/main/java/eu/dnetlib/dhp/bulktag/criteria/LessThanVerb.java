
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author miriam.baglioni
 * @Date 11/11/22
 */
@VerbClass("lesser_than")
public class LessThanVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public LessThanVerb() {
	}

	public LessThanVerb(final String param) {
		this.params = List.of(param);
	}
	public LessThanVerb(final List<String> param) {
		this.params = param;
	}

	public LessThanVerb(final Object param) {
		if(param instanceof String s)
			this.params = List.of(s);
		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
			lista.forEach(l -> params.add(String.valueOf(l)));
	}

	private boolean compare(String param, Object value){
		if(value instanceof String s)
			return s.compareTo(param) < 0;
		if(value instanceof Integer i)
			return i < Integer.parseInt(param);
		return false;
	}

	@Override
	public boolean apply(Object value) {
		if(params.size() == 1){
			if(value instanceof String || value instanceof Integer)
				return compare(params.get(0), value);
			if (value instanceof List<?> list && list.size() == 1){
				return compare(params.get(0), list.get(0));
			}
		}

		return false;
	}

	public List<String> getParam() {
		return params;
	}

	public void setParam(List<String> param) {
		this.params = param;
	}
}
