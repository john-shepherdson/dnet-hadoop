
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 11/11/22
 */
@VerbClass("lesser_than")
public class LessThanVerb implements Selection, Serializable {

	private String param;

	public LessThanVerb() {
	}

	public LessThanVerb(final String param) {
		this.param = param;
	}

	@Override
	public boolean apply(String value) {
		return value.compareTo(param) < 0;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}
}
