
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;

public class ConditionParams implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 2719901844537516110L;

	private String value;
	private String otherValue;

	public String getValue() {
		return value;
	}

	public void setValue(final String value) {
		this.value = value;
	}

	public String getOtherValue() {
		return otherValue;
	}

	public void setOtherValue(final String otherValue) {
		this.otherValue = otherValue;
	}
}
