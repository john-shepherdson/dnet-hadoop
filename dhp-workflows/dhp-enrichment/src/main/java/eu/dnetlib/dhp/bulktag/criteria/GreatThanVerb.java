
package eu.dnetlib.dhp.bulktag.criteria;

import scala.Int;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author miriam.baglioni
 * @Date 11/11/22
 */
@VerbClass("greater_than")
public class GreatThanVerb implements Selection, Serializable {

	private List<String> params = new ArrayList<>();

	public GreatThanVerb() {
	}

	public GreatThanVerb(final String param) {
		this.params = List.of(param);
	}
	public GreatThanVerb(final List<String> param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) {
		if(params.size() == 1) {
			if (value instanceof String s)
				return s.compareTo(params.get(0)) > 0;
			if (value instanceof Integer i)
				return i > Integer.parseInt(params.get(0));
			if(value instanceof List<?> list && list.size() == 1){
				if (list.get(0) instanceof String s)
					return s.compareTo(params.get(0)) > 0;
				if(list.get(0) instanceof Integer i)
					return i > Integer.parseInt(params.get(0));
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
