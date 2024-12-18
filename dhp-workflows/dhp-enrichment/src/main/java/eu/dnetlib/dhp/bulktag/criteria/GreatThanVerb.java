
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 11/11/22
 */
@VerbClass("greater_than")
public class GreatThanVerb implements Selection, Serializable {

	private String param;

	public GreatThanVerb() {
	}

	public GreatThanVerb(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return value.compareTo(param) > 0;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
